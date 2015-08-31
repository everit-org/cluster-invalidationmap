package org.everit.osgi.cache.invalidation.cluster.api;

/**
 * Cluster factory for invalidation map.
 */
public interface InvalidationMapClusterFactory {

  /**
   * Creates the cluster.
   *
   * @param callback
   *          The map invalidation callback that will be invoked due to remote invalidation.
   * @return The cluster.
   */
  InvalidationMapCluster create(InvalidationMapCallback callback);

}
