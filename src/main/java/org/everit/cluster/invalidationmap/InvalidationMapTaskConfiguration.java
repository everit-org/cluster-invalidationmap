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
 * Configurations for the {@link InvalidationMapTaskScheduler}.
 */
public interface InvalidationMapTaskConfiguration {
  /**
   * Returns the invalidation delay.
   *
   * @return The delay in milliseconds.
   * @see #setInvalidateAfterNodeCrashDelay(long)
   */
  long getInvalidateAfterNodeCrashDelay();

  /**
   * Returns the message order check delay.
   *
   * @return The order check delay.
   * @see #setMessageOrderCheckDelay(long)
   */
  long getMessageOrderCheckDelay();

  /**
   * Returns the ping message period.
   *
   * @return The ping message period in milliseconds.
   * @see #setPingPeriod(long)
   */
  long getPingPeriod();

  /**
   * Sets the delay of local cache invalidation after a node crash detected.
   *
   * @param invalidateAfterNodeCrashDelay
   *          The delay in milliseconds.
   */
  void setInvalidateAfterNodeCrashDelay(long invalidateAfterNodeCrashDelay);

  /**
   * Sets the message order check delay in milliseconds. If the cluster detects a potential message
   * loss (or swap) schedules a message order check in the time set.
   *
   * @param messageOrederCheckDelay
   *          The mesage oreder check delay in milliseconds.
   */
  void setMessageOrderCheckDelay(long messageOrederCheckDelay);

  /**
   * Sets the ping message scheduler period.
   *
   * @param period
   *          The period in milliseconds.
   */
  void setPingPeriod(long period);

}
