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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.everit.osgi.cache.jchannel.internal.AbstractInvalidateMap;
import org.jgroups.Channel;

/**
 * Implementation of the {@link AbstractInvalidateMap} with {@link HashMap} as backing container
 * map.
 *
 * @param <K>
 *          The type of keys maintained by this map.
 * @param <V>
 *          The type of mapped values.
 */
public class InvalidateHashMap<K, V> extends AbstractInvalidateMap<K, V> {

  public InvalidateHashMap(final Channel channel) {
    super(channel);
  }

  public InvalidateHashMap(final ConcurrentMap<K, V> map, final Channel channel) {
    super(map, channel);
  }

  @Override
  protected Map<K, V> createWrappedMap() {
    return new HashMap<>();
  }

  @Override
  protected Map<K, V> createWrappedMap(final Map<K, V> from) {
    if (from instanceof HashMap) {
      return from;
    } else {
      Map<K, V> wrappedMap = createWrappedMap();
      wrappedMap.putAll(from);
      return wrappedMap;
    }
  }

}
