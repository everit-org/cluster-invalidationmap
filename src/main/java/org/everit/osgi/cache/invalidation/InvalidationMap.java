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
package org.everit.osgi.cache.invalidation;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapCallback;
import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapCluster;
import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapClusterFactory;

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
    implements Map<K, V> {

  /**
   * Implementation of the {@link InvalidationMapCallback}. It's methods will be invoked by the
   * cluster, if a remote method call command will received.
   *
   * @param <K>
   *          The type of keys maintained by this map.
   * @param <V>The
   *          type of mapped values.
   */
  public static class RemoteMapInvalidator<K, V> implements InvalidationMapCallback {

    /**
     * Wrapped map.
     */
    private final Map<K, V> wrapped;

    public RemoteMapInvalidator(final Map<K, V> wrapped) {
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
   * The wrapped map.
   */
  private Map<K, V> wrapped;

  /**
   * The remote map handler.
   */
  private final InvalidationMapCluster cluster;

  /**
   * Constructs a new {@link InvalidationMap} using provided map instance and cluster factory
   * function.
   *
   * @param map
   *          The map to be set as wrapped map.
   * @param clusterFactory
   *          The cluster factory.
   */
  public InvalidationMap(final Map<K, V> map,
      final InvalidationMapClusterFactory clusterFactory) throws Exception {
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

  public long getPingPeriod() {
    return cluster.getPingPeriod();
  }

  public long getSynchCheckDelay() {
    return cluster.getSynchCheckDelay();
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

  public void setPingPeriod(final long period) {
    cluster.setPingPeriod(period);
  }

  public void setSynchCheckDelay(final long synchCheckDelay) {
    cluster.setSynchCheckDelay(synchCheckDelay);
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  /**
   * Starts the backing cluster.
   *
   * @param stateTimeout
   *          The timeout in milliseconds of the starting method.
   * @throws Exception
   *           If any error occurred.
   */
  public final void start(final long stateTimeout) throws Exception {
    // TODO check if we can wait forever on another thread (and if parameter is 0, stop can be
    // called)
    cluster.start(stateTimeout);
  }

  /**
   * Stops the backing cluster.
   */
  public final void stop() {
    cluster.stop();
  }

  @Override
  public Collection<V> values() {
    return wrapped.values();
  }

}
