package heartbeat;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Heartbeat scheduler has a scheduled thread pool executor with the size of 1. Runnable action is
 * run after each heartBeatInterval milliseconds.
 */
public class HeartBeatScheduler {

  private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

  private final Runnable action;
  private final long heartBeatInterval;
  private ScheduledFuture<?> scheduledTask;

  public HeartBeatScheduler(Runnable action, long heartBeatIntervalMs) {
    this.action = action;
    this.heartBeatInterval = heartBeatIntervalMs;
  }

  public void start() {
    scheduledTask = executor.scheduleWithFixedDelay(action, heartBeatInterval, heartBeatInterval,
        TimeUnit.MILLISECONDS);
  }
}
