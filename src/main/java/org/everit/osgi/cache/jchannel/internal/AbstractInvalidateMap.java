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
package org.everit.osgi.cache.jchannel.internal;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.everit.osgi.cache.jchannel.internal.RemoteMap.RemoteMethods;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.View;

/**
 * This class provides a skeletal implementation of a clustered invalidate capable map. It handles a
 * wrapped map, and a channel for the cluster calls. A remote remove of the key will follows any
 * removing of an object from the wrapped map (invalidation).
 *
 * @param <K>
 *          The type of keys maintained by this map.
 * @param <V>
 *          The type of mapped values.
 */
public abstract class AbstractInvalidateMap<K, V> extends AbstractMap<K, V>
    implements ConcurrentMap<K, V> {

  /**
   * Implementation of the {@link RemoteMap}. It's methods will be invoked by the
   * {@link RemoteMapHandler}, if a remote method call command will received.
   *
   * @param <K>
   *          The type of keys maintained by this map.
   * @param <V>The
   *          type of mapped values.
   */
  public static class RemoteCall<K, V> implements RemoteMap<K, V> {

    private final Map<K, V> wrapped;

    public RemoteCall(final Map<K, V> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public void invalidate(final Object key) {
      wrapped.remove(key);
    }

    @Override
    public void invalidateAll() {
      wrapped.clear();
    }

  }

  /**
   * Extension of the {@link AbstractChannelHandler}.
   * <p>
   * Implements many listener method of the backing channel.
   * </p>
   * <p>
   * Also implements the {@link RemoteMap} interface to able to send remote call commands to the
   * cluster.
   * </p>
   *
   * @param <K>
   *          The type of keys maintained by this map.
   * @param <V>The
   *          type of mapped values.
   */
  private static class RemoteMapHandler<K, V> extends AbstractChannelHandler<RemoteCall<K, V>>
      implements RemoteMap<K, V> {

    public RemoteMapHandler(final Channel channel, final RemoteCall<K, V> server) {
      super(channel, server, METHODS);
      METHODS.checkObject(server);
    }

    @Override
    public void block() {
    }

    @Override
    public void channelClosed(final Channel channel) {
    }

    @Override
    public void channelConnected(final Channel channel) {
    }

    @Override
    public void channelDisconnected(final Channel channel) {
    }

    @Override
    public void getState(final OutputStream output) throws Exception {
    }

    @Override
    public void invalidate(final Object key) {
      callRemoteMethod(METHOD_ID_INVALIDATE, key);
    }

    @Override
    public void invalidateAll() {
      callRemoteMethod(METHOD_ID_INVALIDATE_ALL);
    }

    @Override
    public void receive(final Message msg) {
    }

    @Override
    public void setState(final InputStream input) throws Exception {
    }

    @Override
    public void suspect(final Address suspectedMbr) {
    }

    @Override
    public void unblock() {
    }

    @Override
    public void viewAccepted(final View newView) {
    }

  }

  /**
   * Remote methods. It is constructed from the {@link RemoteMap} implementation {@link RemoteCall}.
   */
  private static final RemoteMethods METHODS = new RemoteMethods(RemoteCall.class);

  /**
   * The wrapped map.
   */
  private final ConcurrentMap<K, V> wrapped;

  /**
   * The remote map handler.
   */
  private final RemoteMapHandler<K, V> remote;

  /**
   * Mutex for the synchronization.
   */
  private final Object mutex = this;

  /**
   * Constructs a new {@link AbstractInvalidateMap} with channel.
   */
  protected AbstractInvalidateMap(final Channel channel) {
    Objects.requireNonNull(channel, "Cannot create AbstractInvalidateMap with null channel");

    wrapped = createWrappedMap();
    RemoteCall<K, V> remoteCall = new RemoteCall<>(wrapped);
    remote = new RemoteMapHandler<>(channel, remoteCall);
  }

  /**
   * Constructs a new {@link AbstractInvalidateMap} using provided map instance.
   */
  protected AbstractInvalidateMap(final ConcurrentMap<K, V> map, final Channel channel) {
    Objects.requireNonNull(channel, "Cannot create AbstractInvalidateMap with null channel");
    Objects.requireNonNull(map, "Cannot create AbstractInvalidateMap with null map");

    wrapped = createWrappedMap(map);
    RemoteCall<K, V> remoteCall = new RemoteCall<>(wrapped);
    remote = new RemoteMapHandler<>(channel, remoteCall);
  }

  @Override
  public void clear() {
    wrapped.clear();
    remote.invalidateAll();
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

  /**
   * Creates an empty {@link ConcurrentMap} and returns it. The map returned will be used as the
   * {@link #wrapped} map.
   *
   * @return An empty map.
   */
  protected abstract ConcurrentMap<K, V> createWrappedMap();

  /**
   * Creates a {@link ConcurrentMap} based on the parameter.
   * <ul>
   * <li>Simply returns the parameter if it is a proper implementation of the {@link Map}</li>
   * <li>Creates an new {@link ConcurrentMap}, copies the content of the parameter map into the
   * created map and returns it otherwise.</li>
   * </ul>
   * The map returned will be used as the {@link #wrapped} map.
   *
   * @param from
   *          Any map.
   *
   * @return The map based on the given parameter.
   */
  protected abstract ConcurrentMap<K, V> createWrappedMap(Map<K, V> from);

  @Override
  public Set<Entry<K, V>> entrySet() {
    return wrapped.entrySet();
  }

  @Override
  public V get(final Object key) {
    return wrapped.get(key);
  }

  /**
   * Returns the backing cluster timeout.
   *
   * @return The cluster timeout
   * @see #setTimeout(long)
   */
  public long getTimeout() {
    return remote.getTimeout();
  }

  /**
   * Returns the backing cluester's blocking update mode flag.
   *
   * @return The flag.
   * @see #setBlockingUpdates(boolean)
   */
  public boolean isBlockingUpdates() {
    return remote.isBlockingUpdates();
  }

  @Override
  public Set<K> keySet() {
    return wrapped.keySet();
  }

  @Override
  public V put(final K key, final V value) {
    synchronized (mutex) {
      boolean containsKey = wrapped.containsKey(key);
      V old = wrapped.put(key, value);
      if (containsKey) {
        remote.invalidate(key);
      }
      return old;
    }
  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    return wrapped.putIfAbsent(key, value);
  }

  @Override
  public V remove(final Object key) {
    synchronized (mutex) {
      boolean containsKey = wrapped.containsKey(key);
      V old = wrapped.remove(key);
      if (containsKey) {
        remote.invalidate(key);
      }
      return old;
    }
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    synchronized (mutex) {
      if (contains(key, value)) {
        wrapped.remove(key);
        remote.invalidate(key);
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public V replace(final K key, final V value) {
    synchronized (mutex) {
      V current = get(key);
      if ((current != null) || containsKey(key)) {
        current = put(key, value);
      }
      return current;
    }
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    synchronized (mutex) {
      if (contains(key, oldValue)) {
        put(key, newValue);
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Sets the backing cluster's blocking update mode. If blocking update is active, the cluster call
   * will wait for response from every node.
   *
   * @param blockingUpdates
   *          Blocking update enable flag.
   */
  public void setBlockingUpdates(final boolean blockingUpdates) {
    remote.setBlockingUpdates(blockingUpdates);
  }

  /**
   * Sets the backing cluster call timeout (until all acknowledgement have been received).
   *
   * @param timeout
   *          The timeout in milliseconds for blocking updates
   */
  public void setTimeout(final long timeout) {
    remote.setTimeout(timeout);
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
    remote.start(stateTimeout);
  }

  /**
   * Stops the backing cluster.
   */
  public final void stop() {
    remote.stop();
  }

  @Override
  public Collection<V> values() {
    return wrapped.values();
  }
}
