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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

/**
 * This class implements a clustered invalidation map. It handles a wrapped map, and a channel for
 * the cluster calls. A remote remove of the key will follows the {@link #remove(Object)} and
 * {@link #clear()} operations on the wrapped map (invalidation).
 *
 * @param <K>
 *          The type of keys maintained by this map.
 * @param <V>
 *          The type of mapped values.
 */
public class InvalidationMap<K, V> extends AbstractMap<K, V>
    implements InvalidationMapConfiguration {

  /**
   * Implementation of the {@link MapInvalidator}. It's methods will be invoked by the cluster, if a
   * remote method call command will received.
   *
   * @param <K>
   *          The type of keys maintained by this map.
   * @param <V>The
   *          type of mapped values.
   */
  private static class RemoteMapInvalidator<K, V> implements MapInvalidator {

    /**
     * Wrapped map.
     */
    private final Map<K, V> wrapped;

    RemoteMapInvalidator(final Map<K, V> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public void invalidate(final Object key) {
      wrapped.remove(key);
      LOGGER.info("Invalidation of key " + key);
    }

    @Override
    public void invalidateAll() {
      wrapped.clear();
      LOGGER.info("Invalidation");
    }

  }

  /**
   * Logger.
   */
  private static final Logger LOGGER = Logger.getLogger(InvalidationMap.class.getName());

  /**
   * The remote map handler.
   */
  private final InvalidationMapCluster cluster;

  /**
   * The wrapped map.
   */
  private final Map<K, V> wrapped;

  /**
   * Constructs a new {@link InvalidationMap} using provided map instance and cluster factory
   * function.
   *
   * @param map
   *          The map to be set as wrapped map.
   * @param clusterFactory
   *          The cluster factory.
   * @see InvalidationMapClusterFactory#create(MapInvalidator)
   */
  public InvalidationMap(final Map<K, V> map,
      final InvalidationMapClusterFactory clusterFactory) {
    Objects.requireNonNull(map, "Cannot create with null map");
    Objects.requireNonNull(clusterFactory, "Cannot create with null clusterFactory");

    wrapped = map;
    cluster = clusterFactory.create(new RemoteMapInvalidator<>(wrapped));
  }

  @Override
  public void clear() {
    wrapped.clear();
    cluster.invalidateAll();
  }

  /**
   * Examine that the {@link #wrapped} map contains the given key with the given value.
   *
   * @param key
   *          The key.
   * @param value
   *          The value.
   * @return <code>true</code> if the {@link #wrapped} map contains the given key with the given
   *         value.
   */
  protected boolean contains(final Object key, final Object value) {
    return wrapped.containsKey(key) && Objects.equals(wrapped.get(key), value);
  }

  @Override
  public boolean containsKey(final Object key) {
    return wrapped.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return wrapped.containsValue(value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return wrapped.entrySet();
  }

  @Override
  public V get(final Object key) {
    return wrapped.get(key);
  }

  @Override
  public long getInvalidateAfterNodeCrashDelay() {
    return cluster.getInvalidateAfterNodeCrashDelay();
  }

  @Override
  public long getMessageOrderCheckDelay() {
    return cluster.getMessageOrderCheckDelay();
  }

  @Override
  public long getPingPeriod() {
    return cluster.getPingPeriod();
  }

  @Override
  public Set<K> keySet() {
    return wrapped.keySet();
  }

  @Override
  public V put(final K key, final V value) {
    V old = wrapped.put(key, value);
    return old;
  }

  @Override
  public V remove(final Object key) {
    V old = wrapped.remove(key);
    cluster.invalidate(key);
    return old;
  }

  @Override
  public void setInvalidateAfterNodeCrashDelay(final long invalidateAfterNodeCrashDelay) {
    cluster.setInvalidateAfterNodeCrashDelay(invalidateAfterNodeCrashDelay);
  }

  @Override
  public void setMessageOrderCheckDelay(final long messageOrdeCheckDelay) {
    cluster.setMessageOrderCheckDelay(messageOrdeCheckDelay);
  }

  @Override
  public void setPingPeriod(final long period) {
    cluster.setPingPeriod(period);
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  /**
   * Starts the backing cluster.
   */
  public void start() {
    cluster.start();
  }

  /**
   * Stops the backing cluster.
   */
  public void stop() {
    cluster.stop();
  }

  @Override
  public Collection<V> values() {
    return wrapped.values();
  }

}
