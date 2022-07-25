package log;

import static java.lang.Math.max;

import command.KVResult;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import kvstore.KVStore;
import log.Instance.InstanceState;

public class Log {

  private final ReentrantLock mu;
  private final Condition cvExecutable;
  private final Condition cvCommitable;
  private final HashMap<Long, Instance> log;
  private long globalLastExecuted = 0;
  private long lastIndex = 0;
  private long lastExecuted = 0;

  public Log() {
    log = new HashMap<>();
    mu = new ReentrantLock();
    cvExecutable = mu.newCondition();
    cvCommitable = mu.newCondition();
  }

  public static boolean insert(HashMap<Long, Instance> log, Instance instance) {
    var i = instance.getIndex();
    var it = log.get(i);
    if (it == null) {
      log.put(i, instance);
      return true;
    }
    if (it.isCommited() || it.isExecuted()) {
      assert (it.getCommand().equals(instance.getCommand())) : "Insert case2";
      return false;
    }
    if (instance.getBallot() > it.getBallot()) {
      log.put(i, instance);
      return false;
    }
    assert
        instance.getBallot() != it.getBallot() || (it.getCommand()
            .equals(instance.getCommand())) : "Insert case3";
    return false;
  }

  public long getLastExecuted() {
    mu.lock();
    try {
      return lastExecuted;
    } finally {
      mu.unlock();
    }
  }

  public void setLastExecuted(long lastExecuted) {
    mu.lock();
    try {
      this.lastExecuted = lastExecuted;
    } finally {
      mu.unlock();
    }
  }

  public long getGlobalLastExecuted() {
    mu.lock();
    try {
      return globalLastExecuted;
    } finally {
      mu.unlock();
    }
  }

  public long advanceLastIndex() {
    mu.lock();
    try {
      return ++lastIndex;
    } finally {
      mu.unlock();
    }
  }

  public boolean isExecutable() {
    Instance found = log.get(lastExecuted + 1);
    return found != null && found.getState() == InstanceState.kCommitted;
  }

  public Instance get(Long index) {
    return log.get(index);
  }

  public void append(Instance instance) {
    mu.lock();
    try {
      long i = instance.getIndex();
      if (i <= globalLastExecuted) {
        return;
      }
      if (insert(log, instance)) {
        lastIndex = max(lastIndex, i);
        cvCommitable.signalAll();
      }
    } finally {
      mu.unlock();
    }
  }

  public void commit(long index) {
    assert (index > 0) : "invalid index";

    mu.lock();
    try {
      var it = log.get(index);
      while (it == null) {
        cvCommitable.await();
        it = log.get(index);
      }
      if (it.isInProgress()) {
        it.setCommited();
      }
      if (isExecutable()) {
        cvExecutable.signal();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      mu.unlock();
    }
  }

  public Map.Entry<Long, KVResult> execute(KVStore kv) {
    mu.lock();
    try {
      while (!isExecutable()) {
        cvExecutable.await();
      }
      var it = log.get(lastExecuted + 1);
      assert it != null;

      assert kv != null;
      KVResult result = kv.execute(it.getCommand());
      it.setExecuted();
      ++lastExecuted;
      return new SimpleEntry<>(it.getClientId(), result);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      mu.unlock();
    }
  }

  public void commitUntil(long leaderLastExecuted, long ballot) {
    assert (leaderLastExecuted >= 0) : "invalid leader_last_executed";
    assert (ballot >= 0) : "invalid ballot";

    mu.lock();
    try {
      for (long i = lastExecuted + 1; i <= leaderLastExecuted; i++) {
        var inst = log.get(i);
        if (inst == null) {
          break;
        }
        assert (ballot >= inst.getBallot()) : "CommitUntil case 2";
        if (inst.getBallot() == ballot) {
          inst.setCommited();
        }
      }
      if (isExecutable()) {
        cvExecutable.signal();
      }
    } finally {
      mu.unlock();
    }
  }

  public void trimUntil(long leaderGlobalLastExecuted) {
    assert (leaderGlobalLastExecuted >= globalLastExecuted) : "invalid leader_global_last_executed";

    mu.lock();
    try {
      while (globalLastExecuted < leaderGlobalLastExecuted) {
        var current = ++globalLastExecuted;
        var inst = log.get(globalLastExecuted);
        assert (inst != null && inst.isExecuted()) : "TrimUntil case 1";
        log.remove(current, inst);
      }
    } finally {
      mu.unlock();
    }
  }

  public ArrayList<Instance> instancesSinceGlobalLastExecuted() {
    mu.lock();
    try {
      ArrayList<Instance> instances = new ArrayList<>();
      for (long i = globalLastExecuted + 1; i <= lastIndex; i++) {
        var inst = log.get(i);
        if (inst != null) {
          instances.add(inst);
        }
      }
      return instances;
    } finally {
      mu.unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    var values = log.values();
    for (var v : values) {
      str.append(v.toString());
    }
    return str.toString();
  }
}
