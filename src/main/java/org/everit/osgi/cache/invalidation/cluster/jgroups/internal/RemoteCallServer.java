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

import org.jgroups.blocks.MethodLookup;

/**
 * Server of the remote method calls.
 */
public final class RemoteCallServer implements RemoteCall {

  /**
   * The cluster handler.
   */
  private final JGroupsInvalidationMapCluster clusterHandler;

  /**
   * Method lookup in the server.
   */
  final MethodLookup methods = new Lookup(this.getClass());

  public RemoteCallServer(final JGroupsInvalidationMapCluster clusterHandler) {
    this.clusterHandler = clusterHandler;
  }

  @Override
  public void bye(final String nodeName, final long startTimeNanos, final long gotMessageNumber) {
    clusterHandler.notifyRemoteCall(nodeName, startTimeNanos, gotMessageNumber);
    clusterHandler.nodeLeft(nodeName);
  }

  @Override
  public void invalidate(final String nodeName, final long startTimeNanos,
      final long gotMessageNumber, final Object key) {
    clusterHandler.notifyRemoteCall(nodeName, startTimeNanos, gotMessageNumber);
    clusterHandler.invalidationCallback.invalidate(key);
  }

  @Override
  public void invalidateAll(final String nodeName, final long startTimeNanos,
      final long gotMessageNumber) {
    clusterHandler.notifyRemoteCall(nodeName, startTimeNanos, gotMessageNumber);
    clusterHandler.invalidationCallback.invalidateAll();
  }

  @Override
  public void ping(final String nodeName, final long startTimeNanos, final long gotMessageNumber) {
    clusterHandler.notifyPing(nodeName, startTimeNanos, gotMessageNumber);
  }

}
