package log;

import static java.lang.Math.max;

import command.Result;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import kvstore.KVStore;
import log.Instance.InstanceState;

public class Log {

  private final long globalLastExecuted = 0;
  private final ReentrantLock mu;
  private final Condition cvExecutable;
  private final Condition cvCommitable;
  private final HashMap<Long, Instance> log;
  private long lastIndex = 0;
  private long lastExecuted = 0;

  public Log() {
    log = new HashMap<>();
    mu = new ReentrantLock();
    cvExecutable = mu.newCondition();
    cvCommitable = mu.newCondition();
  }

  public long getLastExecuted() {
    mu.lock();
    try {
      return lastExecuted;
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

  public void append(Instance instance) {
    mu.lock();
    try {
      long i = instance.getIndex();
      if (i <= globalLastExecuted) {
        return;
      }
      if (instance.isExecuted()) {
        instance.setCommited();
      }

      var it = log.get(i);
      if (it == null) {
        assert (i > lastExecuted) : "Append case 2";
        log.put(i, instance);
        lastIndex = max(lastIndex, i);
        cvCommitable.signalAll();
        return;
      }

      if (it.isCommited() || it.isExecuted()) {
        assert (it.getCommand() == instance.getCommand()) : "Append case 3";
        return;
      }
      if (it.getBallot() < instance.getBallot()) {
        log.put(i, instance);
        return;
      }
      assert it.getBallot() != instance.getBallot() || (it.getCommand()
          == instance.getCommand()) : "Append case 4";
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

  public Map.Entry<Long, Result> execute(KVStore kv) {
    mu.lock();
    try {
      while (!isExecutable()) {
        cvExecutable.await();
      }
      var it = log.get(lastExecuted + 1);
      assert it != null;

      assert kv != null;
      Result result = kv.execute(it.getCommand());
      it.setExecuted();
      ++lastExecuted;
      return new SimpleEntry<>(it.getClientId(), result);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      mu.unlock();
    }
  }


}
