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
package org.everit.cluster.invalidationmap.support.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.everit.cluster.invalidationmap.InvalidationMapConfiguration;
import org.everit.cluster.invalidationmap.MapInvalidator;
import org.everit.cluster.invalidationmap.support.node.NodeRegistry;
import org.everit.cluster.invalidationmap.support.remote.PingSender;

/**
 * Task Scheduler for {@link org.everit.cluster.invalidationmap.InvalidationMapCluster}.
 */
public class TaskScheduler implements InvalidationMapConfiguration {

  /**
   * Logger.
   */
  private static final Logger LOGGER = Logger
      .getLogger(TaskScheduler.class.getName());

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

  // /**
  // * Reschedule a task only if the current delay of the previous schedule is less than the initial
  // * delay multiplied by this constant. Must be less or equal to one, and should be not too close
  // to
  // * zero.
  // */
  // private static final float RESCHEDULE_THRESHOLD_MULTIPLIER = 0.8F;

  /**
   * The map invalidator.
   */
  private final MapInvalidator invalidator;

  /**
   * Node registry.
   */
  private final NodeRegistry nodeRegistry;

  /**
   * Ping sender.
   */
  private final PingSender pingSender;

  /**
   * Backing scheduler.
   */
  private ScheduledExecutorService schedulerService;

  /**
   * Scheduled future of ping sender task.
   */
  private ScheduledFuture<?> pingSenderSchedule;

  /**
   * Scheduled futures of the message order check tasks by node name.
   */
  private final ConcurrentMap<String, ScheduledFuture<?>> messageOrderCheckSchedules = new ConcurrentHashMap<>(); // CS_DISABLE_LINE_LENGTH

  /**
   * Scheduled futures of the local invalidate tasks by node name.
   */
  private final ConcurrentMap<String, ScheduledFuture<?>> invalidateAfterNodeCrashSchedules = new ConcurrentHashMap<>(); // CS_DISABLE_LINE_LENGTH

  /**
   * Period of ping.
   */
  private long pingPeriod = DEFAULT_PING_PERIOD;

  /**
   * Delay of message order check.
   */
  private long messageOrderCheckDelay = DEFAULT_PING_PERIOD;

  /**
   * Delay of the invalidation after node crash suspicion.
   */
  private long invalidateAfterNodeCrashDelay = DEFAULT_INVALIDATE_TIMEOUT;

  /**
   * Creates the task scheduler.
   * @param nodeRegistry
   *          The node registry.
   * @param invalidator
   *          The map invalidator.
   * @param pingSender
   *          The ping sender.
   */
  public TaskScheduler(final NodeRegistry nodeRegistry, final MapInvalidator invalidator,
      final PingSender pingSender) {
    this.nodeRegistry = nodeRegistry;
    this.invalidator = invalidator;
    this.pingSender = pingSender;
    schedulerService = createSchedulerService();
  }

  /**
   * Creates the task that performs the local map invalidation. Task will be invoked after the node
   * crash timeout exceeds.
   *
   * @param nodeName
   *          The name of the node crashed.
   * @return The local map invalidation task.
   * @see #scheduleInvalidateOnNodeCrash(String, boolean)
   */
  public Runnable createInvalidateAfterNodeCrashTask(final String nodeName) {
    return () -> {
      nodeRegistry.remove(nodeName);
      invalidator.invalidateAll();
      LOGGER.warning("Node " + nodeName + " was crashed. Local cache has been invalidated.");
    };
  }

  /**
   * Creates the task that checks whether the got message numbers is in order. Task will be invoked
   * if the message order check delay elapsed after a message lost is suspected on a node.
   *
   * @param nodeName
   *          The name of a suspected node.
   * @param lastPing
   *          The last ping message number.
   * @return The message order check task.
   * @see #scheduleMessageOrderCheck(String, long)
   */
  public Runnable createMessageOrderCheckTask(final String nodeName, final long lastPing) {
    return () -> {
      if (!nodeRegistry.checkMessageOrder(nodeName, lastPing)) {
        nodeRegistry.reset(nodeName, lastPing);
        invalidator.invalidateAll();
        LOGGER.warning("Incomming packet loss detected on node " + nodeName
            + ". Local cache has been invalidated.");
      }
    };
  }

  /**
   * Creates the task that sends a ping message to the members of the cluster. Task will be invoked
   * periodically with fixed delay.
   *
   * @return The ping task.
   * @see #schedulePingSender()
   */
  public Runnable createPingSenderTask() {
    return pingSender::ping;
  }

  private ScheduledExecutorService createSchedulerService() {
    ThreadFactory threadFactory = Executors.defaultThreadFactory();
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
  public synchronized long getMessageOrderCheckDelay() {
    return messageOrderCheckDelay;
  }

  @Override
  public synchronized long getPingPeriod() {
    return pingPeriod;
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
      // was not scheduled previously
      if (reSchedule) {
        // if reschedule is needed
        // FIXME handle RejectedExecutionException?
        invalidateOnNodeCrashFuture = schedulerService.schedule(
            createInvalidateAfterNodeCrashTask(nodeName),
            invalidateAfterNodeCrashDelay, TimeUnit.MILLISECONDS);
      } else {
        // schedule is not needed, nothing to do
        return;
      }
    } else {
      // was scheduled previously
      if (reSchedule) {
        // // schedule if reschedule is needed and current delay exceeds the reschedule threshold
        // // value
        // long currendDelay = prevScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
        // if (currendDelay > invalidateAfterNodeCrashDelay * RESCHEDULE_THRESHOLD_MULTIPLIER) {
        // // current delay does not exceed the threshold value, nothing to do
        // return;
        // }
        // reschedule
        prevScheduledFuture.cancel(false);
        // FIXME handle RejectedExecutionException?
        invalidateOnNodeCrashFuture = schedulerService.schedule(
            createInvalidateAfterNodeCrashTask(nodeName),
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
   * Schedules the message order check task on the node, if previous scheduled task is not executed.
   *
   * @param nodeName
   *          The name of the node on the message order check will be scheduled.
   * @param lastPingMessageNumber
   *          The message number of the last ping.
   *
   * @see InvalidationMapTaskFactory#createMessageOrderCheckTask(String, long)
   */
  public synchronized void scheduleMessageOrderCheck(final String nodeName,
      final long lastPingMessageNumber) {
    if (schedulerService == null) {
      return;
    }
    ScheduledFuture<?> prevScheduledFuture = messageOrderCheckSchedules.get(nodeName);
    if (prevScheduledFuture == null || prevScheduledFuture.isDone()) {
      // schedule message order check if necessary
      LOGGER.info("Scheduling message order check on node " + nodeName);

      // FIXME handle RejectedExecutionException?
      ScheduledFuture<?> messageOrderCheckFuture = schedulerService.schedule(
          createMessageOrderCheckTask(nodeName, lastPingMessageNumber),
          messageOrderCheckDelay, TimeUnit.MILLISECONDS);
      messageOrderCheckSchedules.put(nodeName, messageOrderCheckFuture);
    } else {
      LOGGER.info("Ping check already scheduled on node " + nodeName);
    }
  }

  /**
   * Schedules or reschedules the ping sender task.
   *
   * @see InvalidationMapTaskFactory#createPingSenderTask()
   */
  public synchronized void schedulePingSender() {
    if (schedulerService == null) {
      return;
    }
    if (pingSenderSchedule != null && !pingSenderSchedule.isDone()) {
      pingSenderSchedule.cancel(true);
    }
    // FIXME handle RejectedExecutionException?
    pingSenderSchedule = schedulerService.scheduleAtFixedRate(
        createPingSenderTask(), 0, pingPeriod, TimeUnit.MILLISECONDS);
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
  public synchronized void setMessageOrderCheckDelay(final long messageOrderCheckDelay) {
    if (messageOrderCheckDelay <= 0) {
      throw new IllegalArgumentException("messageOrderCheckDelay must be greater than zero");
    }
    this.messageOrderCheckDelay = messageOrderCheckDelay;
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
    schedulePingSender();
  }

  /**
   * Shutdowns the scheduler.
   */
  public void shutdown() {
    schedulerService.shutdownNow();
    schedulerService = null;
    pingSenderSchedule = null;
    messageOrderCheckSchedules.clear();
    invalidateAfterNodeCrashSchedules.clear();
  }

}
