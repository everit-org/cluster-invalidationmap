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
package org.everit.osgi.cache.jchannel;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.everit.osgi.cache.jchannel.internal.AbstractInvalidateMap;
import org.jgroups.Channel;

/**
 * Implementation of the {@link AbstractInvalidateMap} whit {@link ConcurrentSkipListMap} as backing
 * container map.
 *
 * @param <K>
 *          The type of keys maintained by this map.
 * @param <V>
 *          The type of mapped values.
 */
public class InvalidateSkipListMap<K, V> extends AbstractInvalidateMap<K, V> {

  public InvalidateSkipListMap(final Channel channel) {
    super(channel);
  }

  public InvalidateSkipListMap(final ConcurrentMap<K, V> map, final Channel channel) {
    super(map, channel);
  }

  @Override
  protected ConcurrentMap<K, V> createWrappedMap() {
    return new ConcurrentSkipListMap<>();
  }

  @Override
  protected ConcurrentMap<K, V> createWrappedMap(final Map<K, V> from) {
    if (from instanceof ConcurrentSkipListMap) {
      return (ConcurrentSkipListMap<K, V>) from;
    } else {
      ConcurrentMap<K, V> wrappedMap = createWrappedMap();
      wrappedMap.putAll(from);
      return wrappedMap;
    }
  }

}
