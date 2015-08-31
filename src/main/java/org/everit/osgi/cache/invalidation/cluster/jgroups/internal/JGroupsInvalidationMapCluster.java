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
  private final class PingSenderTask implements Runnable {

    @Override
    public void run() {
      if (!channel.isConnected()) {
        LOGGER.warning("Tried to ping but channel has been closed");
        return;
      }
      MethodCall pingMethodCall = new MethodCall(RemoteCall.METHOD_ID_PING, self.name,
          self.startTimeNanos, messageCounter.get());
      try {
        dispatcher.callRemoteMethods(null, pingMethodCall, ASYNC_REQUEST_OPTIONS);
        LOGGER.info("Ping was sent");
      } catch (Exception e) {
        // FIXME maybe noop map?
        LOGGER.severe("Cannot cast ping");
      }
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
        LOGGER.warning("Incomming packet loss detected on node " + nodeName);
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
   * Default timeout.
   */
  private static final long DEFAULT_PERIOD = 5000;

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
   * Self information.
   */
  final NodeInfo self;

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
  private ScheduledExecutorService pingScheduler;

  /**
   * Scheduled future of ping sender task.
   */
  private ScheduledFuture<?> pingSenderSchedule;

  /**
   * Scheduled futures of the sync check tasks by node name.
   */
  private final ConcurrentMap<String, ScheduledFuture<?>> syncCheckSchedules = new ConcurrentHashMap<>(); // CS_DISABLE_LINE_LENGTH

  /**
   * Period of ping.
   */
  private long pingPeriod = DEFAULT_PERIOD;

  /**
   * Delay of message sync check.
   */
  private long synchCheckDelay = DEFAULT_PERIOD;

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
    self = new NodeInfo(nodeName);
    this.clusterName = clusterName;
    this.invalidationCallback = invalidationCallback;
  }

  /**
   * Calls a method remotely over the channel. Does nothing if {@link #isDroppedOut()} flag is set.
   *
   * @param id
   *          The ID of the method.
   * @param args
   *          The arguments.
   */
  protected void callRemoteMethod(final short id, final Object... args) {
    if (!channel.isConnected()) {
      return;
    }
    try {
      MethodCall call = new MethodCall(id, args);
      dispatcher.callRemoteMethods(null, call, ASYNC_REQUEST_OPTIONS);
      LOGGER.info("Method called: " + call);
    } catch (Exception e) {
      throw new RuntimeException(
          "Cannot call " + server.methods.findMethod(id) + " with parameters "
              + Arrays.toString(args),
          e);
    }
  }

  @Override
  public synchronized long getPingPeriod() {
    return pingPeriod;
  }

  @Override
  public long getSynchCheckDelay() {
    return synchCheckDelay;
  }

  private ScheduledExecutorService initScheduler() {
    String baseName = getClass().getSimpleName() + "-Ping-" + clusterName + "-" + self.name;
    ThreadFactory threadFactory = new DefaultThreadFactory(baseName, true, true);
    ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(
        PING_SCHEDULER_CORE_POOL_SIZE, threadFactory);
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return scheduler;
  }

  @Override
  public void invalidate(final Object key) {
    long counter = messageCounter.incrementAndGet();
    callRemoteMethod(RemoteCall.METHOD_ID_INVALIDATE, self.name, self.startTimeNanos, counter, key);
    LOGGER.info("Invalidate the key in the cache of remote nodes " + key);
  }

  @Override
  public void invalidateAll() {
    long counter = messageCounter.incrementAndGet();
    callRemoteMethod(RemoteCall.METHOD_ID_INVALIDATE_ALL, self.name, self.startTimeNanos, counter);
    LOGGER.info("Invalidate the cache of remote nodes");
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
        SyncCheckTask checkSyncLater = new SyncCheckTask(nodeName, gotMessageNumber);
        synchronized (this) {
          ScheduledFuture<?> oldSyncCheckFuture = syncCheckSchedules.get(nodeName);
          if (oldSyncCheckFuture == null || oldSyncCheckFuture.isDone()) {
            // schedule sync check of necessary
            ScheduledFuture<?> syncCheckFuture = pingScheduler.schedule(checkSyncLater,
                synchCheckDelay, TimeUnit.MILLISECONDS);
            syncCheckSchedules.putIfAbsent(nodeName, syncCheckFuture);
          }
        }
      }
    }

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

  @Override
  public synchronized void setPingPeriod(final long period) {
    if (period <= 0) {
      throw new IllegalArgumentException("period must be greater than null");
    }
    this.pingPeriod = period;
    if (pingSenderSchedule != null && !pingSenderSchedule.isDone()) {
      pingSenderSchedule.cancel(true);
    }
    pingSenderSchedule = pingScheduler.scheduleAtFixedRate(new PingSenderTask(), 0, period,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void setSynchCheckDelay(final long synchCheckDelay) {
    if (synchCheckDelay <= 0) {
      throw new IllegalArgumentException("synchCheckDelay must be greater than null");
    }
    this.synchCheckDelay = synchCheckDelay;
  }

  @Override
  public synchronized void start(final long stateTimeout) {
    if (dispatcher != null) {
      return;
    }
    channel.setDiscardOwnMessages(true);
    channel.setName(self.name);
    nodeRegistry.clear();
    messageCounter.set(0);
    self.startTimeNanos = 9848095388121L; //System.nanoTime();
    dispatcher = new RpcDispatcher(channel, server);
    dispatcher.setMethodLookup(server.methods);
    pingScheduler = initScheduler();
    try {
      toAsync: { // TODO
        channel.connect(clusterName);
      }
      pingSenderSchedule = pingScheduler
          .scheduleAtFixedRate(new PingSenderTask(), 0, pingPeriod, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      pingScheduler.shutdownNow();
      pingSenderSchedule = null;
      syncCheckSchedules.clear();
      dispatcher.stop();
      dispatcher = null;
      channel.close();
      nodeRegistry.clear();
      throw new RuntimeException(e);
    }
    LOGGER.info("Channel was started");
  }

  @Override
  public synchronized void stop() {
    if (dispatcher != null) {
      pingScheduler.shutdownNow();
      pingSenderSchedule = null;
      syncCheckSchedules.clear();
      dispatcher.stop();
      dispatcher = null;
      channel.close();
      nodeRegistry.clear();
      LOGGER.info("Channel was stopped");
    }
  }

}
