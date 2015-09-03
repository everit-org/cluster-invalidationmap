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

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * State of a node.
 */
public class NodeState {

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
