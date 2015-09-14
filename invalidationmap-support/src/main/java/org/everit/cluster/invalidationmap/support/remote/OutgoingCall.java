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
 * Outgoing call. Messages that can be sent on the network.
 */
public interface OutgoingCall extends PingSender {

  /**
   * Sends bye message.
   */
  void bye();

  /**
   * Sends invalidate a key message.
   *
   * @param key
   *          Key to invalidate.
   */
  void invalidate(Object key);

  /**
   * Sends invalidate all of the keys message.
   */
  void invalidateAll();

}
