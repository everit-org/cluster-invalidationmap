/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.cluster.invalidationmap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.jgroups.util.DefaultThreadFactory;

/**
 * Task Scheduler for {@link InvalidationMapCluster}.
 */
public class InvalidationMapTaskScheduler implements InvalidationMapTaskConfiguration {

  /**
   * Logger.
   */
  private static final Logger LOGGER = Logger
      .getLogger(InvalidationMapTaskScheduler.class.getName());

  /**
   * Ping scheduler pool size.
   */
  private static final int PING_SCHEDULER_CORE_POOL_SIZE = 3;

  /**
   * Default period. 5 seconds.
   */
  private static final long DEFAULT_PING_PERIOD = 5 * 1000;

  /**
   * Default invalidate timeout. 30 seconds.
   */
  private static final long DEFAULT_INVALIDATE_TIMEOUT = 30 * 1000;

  /**
   * Reschedule a task only if the current delay of the previous schedule is less than the initial
   * delay multiplied by this constant. Must be less or equal to one, and should be not too close to
   * zero.
   */
  private static final float RESCHEDULE_THRESHOLD_MULTIPLIER = 0.8F;

  /**
   * Task factory that can creates implementation specific tasks.
   */
  private final InvalidationMapTaskFactory taskFactory;

  /**
   * Ping scheduler.
   */
  private ScheduledExecutorService schedulerService;

  /**
   * Scheduled future of ping sender task.
   */
  private ScheduledFuture<?> pingSenderSchedule;

  /**
   * Scheduled futures of the sync check tasks by node name.
   */
  private final ConcurrentMap<String, ScheduledFuture<?>> syncCheckSchedules = new ConcurrentHashMap<>(); // CS_DISABLE_LINE_LENGTH

  /**
   * Scheduled futures of the local invalidate tasks by node name.
   */
  private final ConcurrentMap<String, ScheduledFuture<?>> invalidateAfterNodeCrashSchedules = new ConcurrentHashMap<>(); // CS_DISABLE_LINE_LENGTH

  /**
   * Period of ping.
   */
  private long pingPeriod = DEFAULT_PING_PERIOD;

  /**
   * Delay of message sync check.
   */
  private long syncCheckDelay = DEFAULT_PING_PERIOD;

  /**
   * Delay of the invalidation after node crash suspicion.
   */
  private long invalidateAfterNodeCrashDelay = DEFAULT_INVALIDATE_TIMEOUT;

  public InvalidationMapTaskScheduler(final InvalidationMapTaskFactory taskFactory,
      final String threadBaseName) {
    this.taskFactory = taskFactory;
    schedulerService = createSchedulerService(threadBaseName);
  }

  private ScheduledExecutorService createSchedulerService(final String threadBaseName) {
    ThreadFactory threadFactory = new DefaultThreadFactory(threadBaseName, true, true);
    ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(
        PING_SCHEDULER_CORE_POOL_SIZE, threadFactory);
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return scheduler;
  }

  @Override
  public synchronized long getInvalidateAfterNodeCrashDelay() {
    return invalidateAfterNodeCrashDelay;
  }

  @Override
  public synchronized long getPingPeriod() {
    return pingPeriod;
  }

  @Override
  public synchronized long getSyncCheckDelay() {
    return syncCheckDelay;
  }

  /**
   * Schedules the local invalidate on node crash task.
   *
   * @param nodeName
   *          The name of the node on the task will be invoked.
   * @param reSchedule
   *          Schedules the task if necessary and set to <code>true</code>, cancels the task if
   *          necessary and set to <code>false</code>
   * @see InvalidationMapTaskFactory#createInvalidateAfterNodeCrashTask(String)
   */
  public synchronized void scheduleInvalidateOnNodeCrash(final String nodeName,
      final boolean reSchedule) {
    if (schedulerService == null) {
      return;
    }
    ScheduledFuture<?> prevScheduledFuture = invalidateAfterNodeCrashSchedules.get(nodeName);
    ScheduledFuture<?> invalidateOnNodeCrashFuture;
    if (prevScheduledFuture == null || prevScheduledFuture.isDone()) {
      // not scheduled previously
      if (reSchedule) {
        // if reschedule is needed
        // FIXME handle RejectedExecutionException?
        invalidateOnNodeCrashFuture = schedulerService.schedule(
            taskFactory.createInvalidateAfterNodeCrashTask(nodeName),
            invalidateAfterNodeCrashDelay, TimeUnit.MILLISECONDS);
      } else {
        // schedule is not needed, nothing to do
        return;
      }
    } else {
      // scheduled previously
      if (reSchedule) {
        // schedule if reschedule is needed and current delay exceeds the reschedule threshold value
        long currendDelay = prevScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
        if (currendDelay > invalidateAfterNodeCrashDelay * RESCHEDULE_THRESHOLD_MULTIPLIER) {
          // current delay does not exceed the threshold value, nothing to do
          return;
        }
        // reschedule
        prevScheduledFuture.cancel(false);
        // FIXME handle RejectedExecutionException?
        invalidateOnNodeCrashFuture = schedulerService.schedule(
            taskFactory.createInvalidateAfterNodeCrashTask(nodeName),
            invalidateAfterNodeCrashDelay, TimeUnit.MILLISECONDS);
      } else {
        // schedule is not needed, cancel previous task
        prevScheduledFuture.cancel(false);
        return;
      }
    }
    // put the new scheduled future to the map.
    invalidateAfterNodeCrashSchedules.put(nodeName, invalidateOnNodeCrashFuture);
  }

  /**
   * Schedules or reschedules the ping sender task.
   *
   * @see InvalidationMapTaskFactory#createPingSenderTask()
   */
  public synchronized void schedulePingSenderTask() {
    if (schedulerService == null) {
      return;
    }
    if (pingSenderSchedule != null && !pingSenderSchedule.isDone()) {
      pingSenderSchedule.cancel(true);
    }
    // FIXME handle RejectedExecutionException?
    pingSenderSchedule = schedulerService.scheduleAtFixedRate(
        taskFactory.createPingSenderTask(), 0, pingPeriod, TimeUnit.MILLISECONDS);
  }

  /**
   * Schedules the message sync check task on the node, if previous scheduled task is not executed.
   *
   * @param nodeName
   *          The name of the node on the sync check will be scheduled.
   * @param lastPingMessageNumber
   *          The message number of the last ping.
   *
   * @see InvalidationMapTaskFactory#createSyncCheckTask(String, long)
   */
  public synchronized void scheduleSynchCheckTask(final String nodeName,
      final long lastPingMessageNumber) {
    if (schedulerService == null) {
      return;
    }
    ScheduledFuture<?> prevScheduledFuture = syncCheckSchedules.get(nodeName);
    if (prevScheduledFuture == null || prevScheduledFuture.isDone()) {
      // schedule sync check if necessary
      LOGGER.info("Scheduling ping check on node " + nodeName);

      // FIXME handle RejectedExecutionException?
      ScheduledFuture<?> syncCheckFuture = schedulerService.schedule(
          taskFactory.createSyncCheckTask(nodeName, lastPingMessageNumber),
          syncCheckDelay, TimeUnit.MILLISECONDS);
      syncCheckSchedules.put(nodeName, syncCheckFuture);
    } else {
      LOGGER.info("Ping check already scheduled on node " + nodeName);
    }
  }

  @Override
  public synchronized void setInvalidateAfterNodeCrashDelay(
      final long invalidateAfterNodeCrashDelay) {
    if (invalidateAfterNodeCrashDelay <= 0) {
      throw new IllegalArgumentException("invalidateAfterNodeCrashDelay must be greater than zero");
    }
    if (invalidateAfterNodeCrashDelay <= pingPeriod) {
      throw new IllegalArgumentException(
          "invalidateAfterNodeCrashDelay must be greater than pingPeriod");
    }
    this.invalidateAfterNodeCrashDelay = invalidateAfterNodeCrashDelay;
  }

  @Override
  public synchronized void setPingPeriod(final long period) {
    if (period <= 0) {
      throw new IllegalArgumentException("period must be greater than zero");
    }
    if (pingPeriod >= invalidateAfterNodeCrashDelay) {
      throw new IllegalArgumentException(
          "pingPeriod must be less than invalidateAfterNodeCrashDelay");
    }
    this.pingPeriod = period;
    schedulePingSenderTask();
  }

  @Override
  public synchronized void setSyncCheckDelay(final long syncCheckDelay) {
    if (syncCheckDelay <= 0) {
      throw new IllegalArgumentException("synchCheckDelay must be greater than zero");
    }
    this.syncCheckDelay = syncCheckDelay;
  }

  /**
   * Shutdowns the scheduler.
   */
  public void shutdown() {
    schedulerService.shutdownNow();
    schedulerService = null;
    pingSenderSchedule = null;
    syncCheckSchedules.clear();
    invalidateAfterNodeCrashSchedules.clear();
  }

}
