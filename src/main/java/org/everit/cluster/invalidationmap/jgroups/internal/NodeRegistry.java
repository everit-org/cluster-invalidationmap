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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Node registry.
 */
public class NodeRegistry {

  /**
   * Message list maintenance will be performed if a node has more than
   * {@value #MESSAGE_NUMBER_MAINTENANCE_THRESHOLD} registered message.
   */
  private static final int MESSAGE_NUMBER_MAINTENANCE_THRESHOLD = 250;

  /**
   * Node registry by name.
   */
  private final ConcurrentMap<String, NodeState> nodeState = new ConcurrentHashMap<>();

  /**
   * Maintains the message number registry, and checks whether the node has still out of order
   * messages.
   *
   * @param nodeName
   *          Name of the node.
   * @param messageNumber
   *          Message number.
   * @return <code>true</code> if the node has not out of order messages or node not exists.
   */
  public boolean checkMessageOrder(final String nodeName, final long messageNumber) {
    NodeState state = nodeState.get(nodeName);
    if (state == null) {
      return true;
    }
    long first = state.maintainGotMessageNumbers();
    return first >= messageNumber;
  }

  /**
   * Clears the whole registry.
   */
  public synchronized void clear() {
    nodeState.clear();
  }

  /**
   * Notifies the registry about a ping message. Also checks whether the node has out of order
   * messages.
   *
   * @param nodeName
   *          Name of the node.
   * @param messageNumber
   *          Message number.
   * @return <code>true</code> if message numbers are in order or node not exists.
   */
  public boolean ping(final String nodeName, final long messageNumber) {
    NodeState state = nodeState.get(nodeName);
    // message numbers are in order if
    // the current message number is the expected (last=current)
    // or an older, out of order ping message was got (last>current)
    return state == null || state.gotMessageNumbers.last() >= messageNumber;
  }

  /**
   * Notifies the registry about a non-ping message. Also checks whether the node has out of order
   * messages.
   *
   * @param nodeName
   *          Name of the node.
   * @param messageNumber
   *          Message number.
   * @return <code>true</code> if message numbers are in order or node not exists.
   */
  public boolean receive(final String nodeName, final long messageNumber) {
    NodeState state = nodeState.get(nodeName);
    if (state == null) {
      return true;
    }
    long last;
    synchronized (state) {
      // save the number of the last message, and register the number of the current message
      last = state.gotMessageNumbers.last();
      state.gotMessageNumbers.add(Long.valueOf(messageNumber));
      if (state.gotMessageNumbers.size() > MESSAGE_NUMBER_MAINTENANCE_THRESHOLD) {
        state.maintainGotMessageNumbers();
      }
    }
    // message numbers are in order if
    // the received message is the next expected message (last+1=current) and
    // or an older, out of order message (last+1>current)
    return last + 1 >= messageNumber;
  }

  /**
   * Registers a node if is not registered or was restarted.
   *
   * @param nodeName
   *          Name of the node.
   * @param startTimeNanos
   *          Start time in nanoseconds.
   * @param gotMessageNumber
   *          Message number.
   * @return <code>true</code> if the register of node was performed.
   */
  public boolean registerIfNecessary(
      final String nodeName, final long startTimeNanos, final long gotMessageNumber) {

    NodeState state = nodeState.get(nodeName);
    if (state != null && state.startTimeNanos == startTimeNanos) {
      return false;
    }
    synchronized (this) {
      state = nodeState.get(nodeName);
      if (state != null && state.startTimeNanos == startTimeNanos) {
        return false;
      }
      // in this point the node is not registered or was restarted
      state = new NodeState(startTimeNanos, gotMessageNumber);
      nodeState.put(nodeName, state);
      return true;
    }
  }

  /**
   * Removes a node.
   *
   * @param nodeName
   *          Name of the node.
   */
  public void remove(final String nodeName) {
    nodeState.remove(nodeName);
  }

  /**
   * Resets the message number registry of the node if the node exists.
   *
   * @param nodeName
   *          Name of the node.
   * @param messageNumber
   *          Message number.
   */
  public void reset(final String nodeName, final long messageNumber) {
    NodeState state = nodeState.get(nodeName);
    if (state == null) {
      return;
    }
    synchronized (state) {
      state.gotMessageNumbers.clear();
      state.gotMessageNumbers.add(Long.valueOf(messageNumber));
    }
  }

}
