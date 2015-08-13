package org.everit.osgi.cache.jchannel.internal;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * No operation map.
 *
 * @param <K>
 *          The type of keys maintained by this map
 * @param <V>
 *          The type of mapped values
 */
public class NoOpConcurrentMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V> {

  /**
   * No operation set.
   */
  private final Set<Entry<K, V>> noopEntrySet = new AbstractSet<Entry<K, V>>() {

    @Override
    public boolean add(final Entry<K, V> e) {
      return true;
    }

    @Override
    public boolean addAll(final Collection<? extends Entry<K, V>> c) {
      return true;
    }

    @Override
    public Iterator<java.util.Map.Entry<K, V>> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public int size() {
      return 0;
    }

  };

  @Override
  public Set<Entry<K, V>> entrySet() {
    return noopEntrySet;
  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    return null;
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    return false;
  }

  @Override
  public V replace(final K key, final V value) {
    return null;
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    return false;
  }

}
