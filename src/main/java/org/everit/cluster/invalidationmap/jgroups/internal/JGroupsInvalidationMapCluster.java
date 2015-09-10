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
package org.everit.cluster.invalidationmap.jgroups.internal;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.logging.Logger;

import org.everit.cluster.invalidationmap.InvalidationMapCallback;
import org.everit.cluster.invalidationmap.InvalidationMapCluster;
import org.everit.cluster.invalidationmap.InvalidationMapTaskFactory;
import org.everit.cluster.invalidationmap.InvalidationMapTaskScheduler;
import org.jgroups.JChannel;
import org.jgroups.conf.ProtocolStackConfigurator;

/**
 * Cluster handler for the {@link org.everit.cluster.invalidationmap.InvalidationMap}.
 */
public class JGroupsInvalidationMapCluster
    implements InvalidationMapCluster {

  /**
   * Factory class of the JGroups specific scheduled tasks.
   */
  private class JQroupsInvalidationMapTaskFactory implements InvalidationMapTaskFactory {

    @Override
    public Runnable createInvalidateAfterNodeCrashTask(final String nodeName) {
      return () -> {
        nodeRegistry.remove(nodeName);
        invalidationCallback.invalidateAll();
        LOGGER.warning("Node " + nodeName + " was crashed. Local cache has been invalidated.");
      };
    }

    @Override
    public Runnable createMessageOrderCheckTask(final String nodeName, final long lastPing) {
      return () -> {
        if (!nodeRegistry.checkMessageOrder(nodeName, lastPing)) {
          nodeRegistry.reset(nodeName, lastPing);
          invalidationCallback.invalidateAll();
          LOGGER.warning("Incomming packet loss detected on node " + nodeName
              + ". Local cache has been invalidated.");
        }
      };
    }

    @Override
    public Runnable createPingSenderTask() {
      return remote::ping;
    }

  }

  /**
   * Logger.
   */
  public static final Logger LOGGER = Logger
      .getLogger(JGroupsInvalidationMapCluster.class.getName());
  /**
   * The backing channel.
   */
  final JChannel channel;

  /**
   * The name of the cluster.
   */
  private final String clusterName;

  /**
   * Callback for invalidation of the wrapped map.
   */
  final InvalidationMapCallback invalidationCallback;

  /**
   * Self name.
   */
  final String selfName;

  /**
   * Node registry.
   */
  private final NodeRegistry nodeRegistry = new NodeRegistry();

  /**
   * Remote dispatcher.
   */
  private RemoteCallDispatcher remote = null;

  /**
   * Invalidation map task factory.
   */
  private final InvalidationMapTaskFactory taskFactory = new JQroupsInvalidationMapTaskFactory();

  /**
   * Invalidation map task scheduler.
   */
  private InvalidationMapTaskScheduler taskScheduler = null;

  /**
   * The base name of the task scheduler threads.
   */
  private final String taskSchedulerBaseName;

  /**
   * Creates the instance.
   *
   * @param invalidationCallback
   *          Remote invalidation callback.
   * @param clusterName
   *          The name of the cluster on the channel.
   * @param nodeName
   *          The name of the node.
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
    channel.setDiscardOwnMessages(true);
    channel.setName(nodeName);

    // initialize members
    this.selfName = nodeName;
    this.clusterName = clusterName;
    this.invalidationCallback = invalidationCallback;
    taskSchedulerBaseName = getClass().getSimpleName() + "," + clusterName + "," + nodeName;
  }

  @Override
  public long getInvalidateAfterNodeCrashDelay() {
    return taskScheduler.getInvalidateAfterNodeCrashDelay();
  }

  @Override
  public long getMessageOrderCheckDelay() {
    return taskScheduler.getMessageOrderCheckDelay();
  }

  @Override
  public long getPingPeriod() {
    return taskScheduler.getPingPeriod();
  }

  @Override
  public void invalidate(final Object key) {
    remote.invalidate(key);
  }

  @Override
  public void invalidateAll() {
    remote.invalidateAll();
  }

  /**
   * Notifies the cluster that the node has been left.
   *
   * @param nodeName
   *          The name of the node.
   */
  void nodeLeft(final String nodeName) {
    taskScheduler.scheduleInvalidateOnNodeCrash(nodeName, false);
    nodeRegistry.remove(nodeName);
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
        taskScheduler.scheduleMessageOrderCheck(nodeName, gotMessageNumber);
      }
    }
    taskScheduler.scheduleInvalidateOnNodeCrash(nodeName, true);
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
  public void setInvalidateAfterNodeCrashDelay(final long invalidateAfterNodeCrashDelay) {
    taskScheduler.setInvalidateAfterNodeCrashDelay(invalidateAfterNodeCrashDelay);
  }

  @Override
  public void setMessageOrderCheckDelay(final long syncCheckDelay) {
    taskScheduler.setMessageOrderCheckDelay(syncCheckDelay);
  }

  @Override
  public void setPingPeriod(final long period) {
    taskScheduler.setPingPeriod(period);
  }

  @Override
  public synchronized void start() {
    if (remote != null) {
      return;
    }
    nodeRegistry.clear();
    remote = new RemoteCallDispatcher(this);
    taskScheduler = new InvalidationMapTaskScheduler(taskFactory, taskSchedulerBaseName);
    try {
      channel.connect(clusterName);
      taskScheduler.schedulePingSender();
    } catch (Exception e) {
      stop(false);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Cannot start invalidation map cluster", e);
    }
    LOGGER.info("Channel was started");
  }

  @Override
  public synchronized void stop() {
    if (remote != null) {
      stop(true);
      LOGGER.info("Channel was stopped");
    }
  }

  /**
   * Stops the clustered operation.
   *
   * @param sendByeMessage
   *          Send the bye message before closing the channel.
   */
  private void stop(final boolean sendByeMessage) {
    taskScheduler.shutdown();
    taskScheduler = null;
    remote.stop(sendByeMessage);
    remote = null;
    channel.close();
    nodeRegistry.clear();
  }

}
