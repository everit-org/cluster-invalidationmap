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

/**
 * Factory class of the scheduled tasks for {@link InvalidationMapCluster}.
 */
public interface InvalidationMapTaskFactory {

  /**
   * Creates the task that performs the local map invalidation. Task will be invoked after the node
   * crash timeout exceeds.
   *
   * @param nodeName
   *          The name of the node crashed.
   * @return The local map invalidation task.
   * @see InvalidationMapTaskScheduler#scheduleInvalidateOnNodeCrash(String, boolean)
   */
  Runnable createInvalidateAfterNodeCrashTask(final String nodeName);

  /**
   * Creates the task that checks whether the got message numbers is in order. Task will be invoked
   * if the message order check delay elapsed after a message lost is suspected on a node.
   *
   * @param nodeName
   *          The name of a suspected node.
   * @param lastPingMessageNumber
   *          The last ping message number.
   * @return The message order check task.
   * @see InvalidationMapTaskScheduler#scheduleMessageOrderCheck(String, long)
   */
  Runnable createMessageOrderCheckTask(final String nodeName, final long lastPingMessageNumber);

  /**
   * Creates the task that sends a ping message to the members of the cluster. Task will be invoked
   * periodically with fixed delay.
   *
   * @return The ping task.
   * @see InvalidationMapTaskScheduler#schedulePingSender()
   */
  Runnable createPingSenderTask();
}
