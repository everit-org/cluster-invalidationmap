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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * No operation map.
 *
 * @param <K>
 *          The type of keys maintained by this map
 * @param <V>
 *          The type of mapped values
 */
public class NoOpMap<K, V> extends AbstractMap<K, V> implements Map<K, V> {

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
    public Iterator<Entry<K, V>> iterator() {
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

}
