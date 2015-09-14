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
package org.everit.cluster.invalidationmap.support.remote;

/**
 * Incoming call interface. It declares the remote invokable methods.
 */
public interface IncomingCall {

  /**
   * Handles bye message.
   *
   * @param nodeName
   *          The name of the sender node.
   * @param startTimeNanos
   *          The start time of the node in nanoseconds.
   * @param gotMessageNumber
   *          The incremented message number.
   */
  void bye(String nodeName, long startTimeNanos, long gotMessageNumber);

  /**
   * Invalidates a key.
   *
   * @param nodeName
   *          The name of the sender node.
   * @param startTimeNanos
   *          The start time of the node in nanoseconds.
   * @param gotMessageNumber
   *          The incremented message number.
   * @param key
   *          The key.
   */
  void invalidate(String nodeName, long startTimeNanos, long gotMessageNumber, Object key);

  /**
   * Invalidates the whole map.
   *
   * @param nodeName
   *          The name of the sender node.
   * @param startTimeNanos
   *          The start time of the node in nanoseconds.
   * @param gotMessageNumber
   *          The incremented message number.
   */
  void invalidateAll(String nodeName, long startTimeNanos, long gotMessageNumber);

  /**
   * Handles ping message.
   *
   * @param nodeName
   *          The name of the sender node.
   * @param startTimeNanos
   *          The start time of the node in nanoseconds.
   * @param gotMessageNumber
   *          The previously sent message number.
   */
  void ping(String nodeName, long startTimeNanos, long gotMessageNumber);

}
