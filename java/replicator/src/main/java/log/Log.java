package log;

import java.util.HashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import log.Instance.InstanceState;

public class Log {

  private long lastIndex = 0;
  private long lastExecuted = 0;
  private long globalLastExecuted = 0;
  private ReentrantLock mu;
  private Condition cv;
  private HashMap<Long, Instance> log;

  public Log(){
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
}
