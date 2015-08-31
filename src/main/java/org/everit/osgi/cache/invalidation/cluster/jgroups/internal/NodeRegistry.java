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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Node registry.
 */
public class NodeRegistry {

  /**
   * State of a node.
   */
  private static class NodeState {

    /**
     * Start time in nanoseconds.
     */
    public long startTimeNanos;

    // NOTE the set is a sorted set, and concurrent, and the backing code uses this nature!
    // If it's needed switch the implementation carefully!
    /**
     * Got message numbers for lost message detection. Must be sorted and concurrent.
     */
    public final ConcurrentSkipListSet<Long> gotMessageNumbers = new ConcurrentSkipListSet<>();

    public NodeState(final long startTimeNanos, final long firstMessageNumber) {
      this.startTimeNanos = startTimeNanos;
      gotMessageNumbers.add(Long.valueOf(firstMessageNumber));
    }

    /**
     * Removes the consecutive numbers from the header of the given {@link ConcurrentSkipListSet}.
     *
     * @param numbers
     *          The numbers.
     * @return The first element after the maintain operation.
     */
    public synchronized long maintainGotMessageNumbers() {

      if (gotMessageNumbers.size() < 2) {
        // nothing to maintain if size under 2, simply return the first number
        return gotMessageNumbers.first().longValue();
      }

      // NOTE the gotMessageNumbers set has ascending order

      // search the end of the consecutive numbers on the head of the message numbers.
      Iterator<Long> numberIt;
      numberIt = gotMessageNumbers.iterator();
      long endingNumber = numberIt.next().longValue();
      do {
        long current = numberIt.next().longValue();
        if (endingNumber + 1 != current) {
          // we found the end of the consecutive number block
          // if the previous (ending candidate) number is not one less than current number
          break;
        }
        endingNumber = current;
      } while (numberIt.hasNext());

      // remove all numbers less then previously found ending number
      numberIt = gotMessageNumbers.iterator();
      while (numberIt.hasNext()) {
        long current = numberIt.next().longValue();
        if (current < endingNumber) {
          numberIt.remove();
        } else {
          break;
        }
      }

      // return the first number
      return gotMessageNumbers.first().longValue();
    }
  }

  /**
   * Node registry by name.
   */
  private final ConcurrentMap<String, NodeState> nodeState = new ConcurrentHashMap<>();

  /**
   * Maintains the message number registry, and checks if the node is still out of sync.
   *
   * @param nodeName
   *          Name of the node.
   * @param messageNumber
   *          Message number.
   * @return <code>true</code> if the node is out of sync or node not exists.
   */
  public boolean checkSync(final String nodeName, final long messageNumber) {
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
  public void clear() {
    synchronized (this) {
      nodeState.clear();
    }
  }

  /**
   * Notifies the registry about a ping message. Also checks if the node is in sync.
   *
   * @param nodeName
   *          Name of the node.
   * @param messageNumber
   *          Message number.
   * @return <code>true</code> if message number is in sync or node not exists.
   */
  public boolean ping(final String nodeName, final long messageNumber) {
    NodeState state = nodeState.get(nodeName);
    return state == null || state.gotMessageNumbers.last() >= messageNumber;
  }

  /**
   * Notifies the registry about a non-ping message. Also checks if the node is in sync.
   *
   * @param nodeName
   *          Name of the node.
   * @param messageNumber
   *          Message number.
   * @return <code>true</code> if message number is in sync or node not exists.
   */
  public boolean receive(final String nodeName, final long messageNumber) {
    NodeState state = nodeState.get(nodeName);
    if (state == null) {
      return true;
    }
    long last;
    synchronized (state) {
      last = state.gotMessageNumbers.last();
      state.gotMessageNumbers.add(Long.valueOf(messageNumber));
    }
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
