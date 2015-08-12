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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.jgroups.blocks.MethodLookup;

/**
 * Remote map interface. It declares the remote invokable methods.
 *
 * @param <K>
 *          The type of keys maintained by this map.
 * @param <V>
 *          The type of mapped values.
 */
public interface RemoteMap<K, V> {

  /**
   * Stores the remote methods from the {@link RemoteMap} implementation. It also implements the
   * {@link MethodLookup} through the gathered methods can be asked.
   */
  class RemoteMethods implements MethodLookup {

    /**
     * Name of the method {@link RemoteMap#invalidate(Object)}.
     */
    private static final String METHOD_NAME_INVALIDATE = "invalidate";

    /**
     * Name of the method {@link RemoteMap#invalidateAll()}.
     */
    private static final String METHOD_NAME_INVALIDATE_ALL = "invalidateAll";

    /**
     * Method map assigns methods toe ID.
     */
    private final Map<Short, Method> methods = new HashMap<Short, Method>(2);

    /**
     * Class from the instanci is constructed.
     */
    @SuppressWarnings("rawtypes")
    private final Class<? extends RemoteMap> oClass;

    /**
     * Creates the instance based on the given object's class.
     *
     * @param oClass
     *          The class of the object from the necessary methods will be gathered.
     */
    public RemoteMethods(@SuppressWarnings("rawtypes") final Class<? extends RemoteMap> oClass) {

      Objects.requireNonNull(oClass, "Cannot gather methods from null");

      this.oClass = oClass;
      try {
        methods.put(
            Short.valueOf(METHOD_ID_INVALIDATE),
            oClass.getMethod(METHOD_NAME_INVALIDATE, Object.class));
        methods.put(
            Short.valueOf(METHOD_ID_INVALIDATE_ALL),
            oClass.getMethod(METHOD_NAME_INVALIDATE_ALL));
      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Cannot gather methods", e);
      }
    }

    /**
     * Checks that this instance is constructed from the class of the given object.
     *
     * @param o
     *          Any object.
     * @throws RuntimeException
     *           If the check fails.
     */
    public void checkObject(final Object o) {
      Objects.requireNonNull(o, "Parameter object cannot be null");

      if (o.getClass() != oClass) {
        throw new RuntimeException("Method can call only from" + oClass.getName());
      }
    }

    @Override
    public Method findMethod(final short id) {
      return methods.get(Short.valueOf(id));
    }
  }

  /**
   * ID of the method {@link #invalidate(Object)}.
   */
  short METHOD_ID_INVALIDATE = 1;

  /**
   * ID of the method {@link #invalidateAll()}.
   */
  short METHOD_ID_INVALIDATE_ALL = 2;

  /**
   * Invalidates a key.
   *
   * @param key
   *          The key.
   */
  void invalidate(Object key);

  /**
   * Invalidate the whole map.
   */
  void invalidateAll();

}
