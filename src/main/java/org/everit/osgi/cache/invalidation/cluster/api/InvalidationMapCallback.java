package org.everit.osgi.cache.invalidation.cluster.api;

/**
 * Map invalidation methods.
 */
public interface InvalidationMapCallback {

  /**
   * Invalidates only the specified key in the map.
   *
   * @param key
   *          Key to be invalidated.
   */
  void invalidate(Object key);

  /**
   * Invalidate the full map.
   */
  void invalidateAll();
}
