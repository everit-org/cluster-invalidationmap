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
  public void invalidate(final String nodeName, final long startTimeNanos,
      final long gotMessageNumber,      final Object key) {
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
