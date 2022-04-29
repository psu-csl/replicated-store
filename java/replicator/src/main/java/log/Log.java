package log;

import command.Result;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import kvstore.KVStore;
import log.Instance.InstanceState;

public class Log {

  private long lastIndex = 0;
  private long lastExecuted = 0;
  private final long globalLastExecuted = 0;
  private final ReentrantLock mu;
  private final Condition cv;
  private final HashMap<Long, Instance> log;

  public Log() {
    log = new HashMap<>();
    mu = new ReentrantLock();
    cv = mu.newCondition();
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

  public Map.Entry<Long, Result> execute(KVStore kv) {
    assert (isExecutable());
    mu.lock();
    try {
      Instance instance = log.get(lastExecuted + 1);
      Result result = kv.execute(instance.getCommand());
      ++lastExecuted;
      return new SimpleEntry<>(instance.getClientId(), result);
    } finally {
      mu.unlock();
    }
  }


}
