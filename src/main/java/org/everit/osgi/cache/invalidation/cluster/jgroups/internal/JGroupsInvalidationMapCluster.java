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
package org.everit.osgi.cache.invalidation.cluster.jgroups.internal;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.logging.Logger;

import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapCallback;
import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapCluster;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.util.DefaultThreadFactory;

/**
 * Cluster handler for the {@link org.everit.osgi.cache.invalidation.InvalidationMap}.
 */
public class JGroupsInvalidationMapCluster implements InvalidationMapCluster {

  /**
   * Ping message sender task. It send asynchronously a ping message to all of the nodes.
   */
  private final class LocalInvalidateAfterNodeCrashTask implements Runnable {

    /**
     * Name of the node that causes the invalidation.
     */
    private final String nodeName;

    public LocalInvalidateAfterNodeCrashTask(final String nodeName) {
      super();
      this.nodeName = nodeName;
    }

    @Override
    public void run() {
      invalidationCallback.invalidateAll();
      LOGGER.warning("Node " + nodeName + " was crashed. Invalidate local cache");
    }

  }

  /**
   * Ping message sender task. It send asynchronously a ping message to all of the nodes.
   */
  private final class PingSenderTask implements Runnable {

    @Override
    public void run() {
      callRemoteMethod(RemoteCall.METHOD_ID_PING, false);
      LOGGER.info("Ping was sent");
    }

  }

  /**
   * Checks whether the ping message number and got message numbers are consistent. Can detect
   * packet loss.
   */
  private final class SyncCheckTask implements Runnable {

    /**
     * Name of the node to check.
     */
    private final String nodeName;

    /**
     * The last ping counter value.
     */
    private final long lastPing;

    public SyncCheckTask(final String nodeName, final long lastPing) {
      this.nodeName = nodeName;
      this.lastPing = lastPing;
    }

    @Override
    public void run() {
      if (!nodeRegistry.checkSync(nodeName, lastPing)) {
        nodeRegistry.reset(nodeName, lastPing);
        invalidationCallback.invalidateAll();
        LOGGER.warning("Incomming packet loss detected on node " + nodeName
            + ". Local cache has been invalidated");
      }
    }
  }

  /**
   * Logger.
   */
  private static final Logger LOGGER = Logger
      .getLogger(JGroupsInvalidationMapCluster.class.getName());

  /**
   * Ping scheduler pool size.
   */
  private static final int PING_SCHEDULER_CORE_POOL_SIZE = 3;

  /**
   * Default period. 5 seconds.
   */
  private static final long DEFAULT_PING_PERIOD = 5000;

  /**
   * Default invalidate timeout. 30 seconds.
   */
  private static final long DEFAULT_INVALIDATE_TIMEOUT = 30000;

  /**
   * Reschedule a task only if the current delay of the previous schedule is less than the initial
   * delay multiplied by this constant. Must be less or equal to one, and should be not too close to
   * zero.
   */
  private static final float RESCHEDULE_THRESHOLD_MULTIPLIER = 0.95F;

  /**
   * Asynchronous request options.
   */
  private static final RequestOptions ASYNC_REQUEST_OPTIONS = new RequestOptions(
      ResponseMode.GET_NONE, 0);

  /**
   * The backing channel.
   */
  private final Channel channel;

  /**
   * The name of the cluster.
   */
  private final String clusterName;

  /**
   * The remote method call dispatcher on the top of the {@link #channel}.
   */
  private RpcDispatcher dispatcher = null;

  /**
   * Server instance.
   */
  private final RemoteCallServer server = new RemoteCallServer(this);

  /**
   * Callback.
   */
  final InvalidationMapCallback invalidationCallback;

  /**
   * Self name.
   */
  final String nodeName;

  /**
   * Time stamp of the clustered operation start.
   */
  long startTimeNanos;

  /**
   * Node registry.
   */
  private final NodeRegistry nodeRegistry = new NodeRegistry();

  /**
   * Message counter.
   */
  private final AtomicLong messageCounter = new AtomicLong();

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

  /**
   * Creates the instance.
   *
   * @param invalidationCallback
   *          Remote invalidation callback.
   * @param clusterName
   *          The name of the cluster on the channel.
   * @param protocols
   *          The channel protocols.
   */
  public JGroupsInvalidationMapCluster(final InvalidationMapCallback invalidationCallback,
      final String clusterName, final String nodeName, final ProtocolStackConfigurator protocols) {

    // argument check
    Objects.requireNonNull(invalidationCallback, "Cannot create with null invalidationCallback");
    Objects.requireNonNull(clusterName, "Cannot create with null clusterName");
    Objects.requireNonNull(nodeName, "Cannot create with null nodeName");
    Objects.requireNonNull(protocols, "Cannot create with null protocols");
    if (clusterName.isEmpty()) {
      throw new IllegalArgumentException("Cannot create with empty clusterName");
    }
    if (nodeName.isEmpty()) {
      throw new IllegalArgumentException("Cannot create with empty nodeName");
    }
    if (protocols.getProtocolStack() == null || protocols.getProtocolStack().isEmpty()) {
      throw new IllegalArgumentException("Cannot create with empty protocols");
    }

    // try to create channel
    try {
      channel = new JChannel(protocols);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e; // rethrow runtime exception
      }
      throw new RuntimeException("Cannot create the cluster", e);
    }

    // initialize members
    this.nodeName = nodeName;
    this.clusterName = clusterName;
    this.invalidationCallback = invalidationCallback;
  }

  /**
   * Calls a method remotely over the channel. Does nothing if {@link #isDroppedOut()} flag is set.
   *
   * @param id
   *          The ID of the method.
   * @param incrementCounter
   *          Set <code>true</code> if the {@link #messageCounter} must be incremented.
   * @param args
   *          The arguments.
   */
  protected void callRemoteMethod(final short id, final boolean incrementCounter,
      final Object... args) {
    if (!channel.isConnected()) {
      return;
    }
    try {
      long counter = incrementCounter ? messageCounter.incrementAndGet() : messageCounter.get();
      Object[] callArgs = new Object[args.length + RemoteCall.MANDATORY_PARAMETER_COUNT];
      callArgs[0] = nodeName;
      callArgs[1] = Long.valueOf(startTimeNanos);
      callArgs[2] = Long.valueOf(counter);
      if (args.length > 0) {
        System.arraycopy(args, 0, callArgs, RemoteCall.MANDATORY_PARAMETER_COUNT, args.length);
      }
      MethodCall call = new MethodCall(id, callArgs);
      dispatcher.callRemoteMethods(null, call, ASYNC_REQUEST_OPTIONS);
      LOGGER.info("Method called: " + server.methods.findMethod(id).getName() + " "
          + Arrays.toString(callArgs));
    } catch (Exception e) {
      throw new RuntimeException(
          "Cannot call " + server.methods.findMethod(id) + " with parameters "
              + Arrays.toString(args),
          e);
    }
  }

  @Override
  public long getInvalidateAfterNodeCrashDelay() {
    return invalidateAfterNodeCrashDelay;
  }

  public ConcurrentMap<String, ScheduledFuture<?>> getInvalidateAfterNodeCrashSchedules() {
    return invalidateAfterNodeCrashSchedules;
  }

  @Override
  public long getPingPeriod() {
    return pingPeriod;
  }

  @Override
  public long getSyncCheckDelay() {
    return syncCheckDelay;
  }

  private ScheduledExecutorService initScheduler() {
    String baseName = getClass().getSimpleName() + "-Ping-" + clusterName + "-" + nodeName;
    ThreadFactory threadFactory = new DefaultThreadFactory(baseName, true, true);
    ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(
        PING_SCHEDULER_CORE_POOL_SIZE, threadFactory);
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return scheduler;
  }

  @Override
  public void invalidate(final Object key) {
    callRemoteMethod(RemoteCall.METHOD_ID_INVALIDATE, true, key);
    LOGGER.info("Invalidate the key in the cache of remote nodes " + key);
  }

  @Override
  public void invalidateAll() {
    callRemoteMethod(RemoteCall.METHOD_ID_INVALIDATE_ALL, true);
    LOGGER.info("Invalidate the cache of remote nodes");
  }

  /**
   * Notifies the cluster that the node has been left.
   *
   * @param nodeName
   *          The name of the node.
   */
  void nodeLeft(final String nodeName) {
    scheduleInvalidateOnNodeCrash(nodeName, false);
    LOGGER.info("Node " + nodeName + " left");
  }

  /**
   * Notifies the handler about a message.
   *
   * @param nodeName
   *          The name of the source node.
   * @param startTimeNanos
   *          The node start time in nanoseconds.
   * @param gotMessageNumber
   *          The number of the got non-ping message.
   * @param notifyRegistry
   *          Node registry notify function.
   */
  private void notifyMessage(final String nodeName, final long startTimeNanos,
      final long gotMessageNumber, final BiFunction<String, Long, Boolean> notifyRegistry) {

    if (nodeRegistry.registerIfNecessary(nodeName, startTimeNanos, gotMessageNumber)) {
      LOGGER.info("Node joined or restarted " + nodeName + ":" + startTimeNanos);
    } else {
      if (!notifyRegistry.apply(nodeName, gotMessageNumber)) {
        scheduleSynchCheckTaskIfNecessary(nodeName, gotMessageNumber);
      }
    }
    scheduleInvalidateOnNodeCrash(nodeName, true);
  }

  /**
   * Notifies the handler about a ping message.
   *
   * @param nodeName
   *          The name of the source node.
   * @param startTimeNanos
   *          The node start time in nanoseconds.
   * @param gotMessageNumber
   *          The number of the got non-ping message.
   */
  void notifyPing(final String nodeName, final long startTimeNanos, final long gotMessageNumber) {
    LOGGER.info("Ping notify (" + nodeName + ":" + gotMessageNumber + ")");
    notifyMessage(nodeName, startTimeNanos, gotMessageNumber, nodeRegistry::ping);
  }

  /**
   * Notifies the handler about a remote call message.
   *
   * @param nodeName
   *          The name of the source node.
   * @param startTimeNanos
   *          The node start time in nanoseconds.
   * @param gotMessageNumber
   *          The number of the got non-ping message.
   */
  void notifyRemoteCall(final String nodeName, final long startTimeNanos,
      final long gotMessageNumber) {
    LOGGER.info("Remote call notify (" + nodeName + ":" + gotMessageNumber + ")");
    notifyMessage(nodeName, startTimeNanos, gotMessageNumber, nodeRegistry::receive);
  }

  private synchronized void scheduleInvalidateOnNodeCrash(final String nodeName,
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
            new LocalInvalidateAfterNodeCrashTask(nodeName), invalidateAfterNodeCrashDelay,
            TimeUnit.MILLISECONDS);
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
          // current delay does not exceed the threshold value, nothind to do
          return;
        }
        // reschedule
        prevScheduledFuture.cancel(false);
        // FIXME handle RejectedExecutionException?
        invalidateOnNodeCrashFuture = schedulerService.schedule(
            new LocalInvalidateAfterNodeCrashTask(nodeName),
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

  private synchronized void schedulePingSenderTask(final long period) {
    if (schedulerService == null) {
      return;
    }
    if (pingSenderSchedule != null && !pingSenderSchedule.isDone()) {
      pingSenderSchedule.cancel(true);
    }
    // FIXME handle RejectedExecutionException?
    pingSenderSchedule = schedulerService.scheduleAtFixedRate(new PingSenderTask(), 0, period,
        TimeUnit.MILLISECONDS);
  }

  private synchronized void scheduleSynchCheckTaskIfNecessary(final String nodeName,
      final long gotMessageNumber) {
    if (schedulerService == null) {
      return;
    }
    ScheduledFuture<?> prevScheduledFuture = syncCheckSchedules.get(nodeName);
    if (prevScheduledFuture == null || prevScheduledFuture.isDone()) {
      LOGGER.info("Scheduling ping check on node " + nodeName);
      // schedule sync check of necessary
      SyncCheckTask syncCheckLater = new SyncCheckTask(nodeName, gotMessageNumber);
      // FIXME handle RejectedExecutionException?
      ScheduledFuture<?> syncCheckFuture = schedulerService.schedule(syncCheckLater,
          syncCheckDelay, TimeUnit.MILLISECONDS);
      syncCheckSchedules.put(nodeName, syncCheckFuture);
    } else {
      LOGGER.info("Ping check already scheduled on node " + nodeName);
    }
  }

  @Override
  public void setInvalidateAfterNodeCrashDelay(final long invalidateAfterNodeCrashDelay) {
    this.invalidateAfterNodeCrashDelay = invalidateAfterNodeCrashDelay;
  }

  @Override
  public void setPingPeriod(final long period) {
    if (period <= 0) {
      throw new IllegalArgumentException("period must be greater than null");
    }
    this.pingPeriod = period;
    schedulePingSenderTask(period);
  }

  @Override
  public void setSyncCheckDelay(final long syncCheckDelay) {
    if (syncCheckDelay <= 0) {
      throw new IllegalArgumentException("synchCheckDelay must be greater than null");
    }
    this.syncCheckDelay = syncCheckDelay;
  }

  @Override
  public synchronized void start() {
    if (dispatcher != null) {
      return;
    }
    channel.setDiscardOwnMessages(true);
    channel.setName(nodeName);
    nodeRegistry.clear();
    messageCounter.set(0);
    startTimeNanos = System.nanoTime();
    dispatcher = new RpcDispatcher(channel, server);
    dispatcher.setMethodLookup(server.methods);
    schedulerService = initScheduler();
    try {
      channel.connect(clusterName);
      schedulePingSenderTask(pingPeriod);
    } catch (Exception e) {
      schedulerService.shutdownNow();
      schedulerService = null;
      pingSenderSchedule = null;
      syncCheckSchedules.clear();
      invalidateAfterNodeCrashSchedules.clear();
      dispatcher.stop();
      channel.close();
      nodeRegistry.clear();
      dispatcher = null;
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Cannot start invalidation map cluster", e);
    }
    LOGGER.info("Channel was started");
  }

  @Override
  public synchronized void stop() {
    if (dispatcher != null) {
      callRemoteMethod(RemoteCall.METHOD_ID_BYE, true);
      RpcDispatcher d = dispatcher;
      dispatcher = null;
      schedulerService.shutdownNow();
      schedulerService = null;
      pingSenderSchedule = null;
      syncCheckSchedules.clear();
      invalidateAfterNodeCrashSchedules.clear();
      d.stop();
      channel.close();
      nodeRegistry.clear();
      LOGGER.info("Channel was stopped");
    }
  }

}
