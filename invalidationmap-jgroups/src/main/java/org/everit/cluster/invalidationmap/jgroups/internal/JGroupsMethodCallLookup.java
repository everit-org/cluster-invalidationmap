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
package org.everit.cluster.invalidationmap.jgroups.internal;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.everit.cluster.invalidationmap.support.remote.IncomingCall;
import org.jgroups.blocks.MethodLookup;

/**
 * Stores the remote methods from the {@link IncomingCall} implementation. It also implements the
 * {@link MethodLookup} through the gathered methods can be asked.
 */
class JGroupsMethodCallLookup implements MethodLookup {

  /**
   * Count of the remote methods.
   */
  private static final int METHOD_COUNT = 4;

  /**
   * Name of the method {@link IncomingCall#invalidate(String, long, long, Object)}.
   */
  private static final String METHOD_NAME_INVALIDATE = "invalidate";

  /**
   * Name of the method {@link IncomingCall#invalidateAll(String, long, long)}.
   */
  private static final String METHOD_NAME_INVALIDATE_ALL = "invalidateAll";

  /**
   * Name of the method {@link IncomingCall#ping(String, long, long)}.
   */
  private static final String METHOD_NAME_PING = "ping";

  /**
   * Name of the method {@link IncomingCall#bye(String, long, long)}.
   */
  private static final String METHOD_NAME_BYE = "bye";

  /**
   * Method map assigns methods toe ID.
   */
  private final Map<Short, Method> methods = new HashMap<Short, Method>(METHOD_COUNT);

  /**
   * Creates the instance based on the given object's class.
   *
   * @param oClass
   *          The class of the object from the necessary methods will be gathered.
   */
  JGroupsMethodCallLookup(final Class<? extends IncomingCall> oClass) {

    Objects.requireNonNull(oClass, "Cannot gather methods from null");

    try {
      methods.put(
          Short.valueOf(JGroupsMethodCallDispatcher.METHOD_ID_INVALIDATE),
          oClass.getMethod(METHOD_NAME_INVALIDATE, String.class, Long.TYPE, Long.TYPE,
              Object.class));
      methods.put(
          Short.valueOf(JGroupsMethodCallDispatcher.METHOD_ID_INVALIDATE_ALL),
          oClass.getMethod(METHOD_NAME_INVALIDATE_ALL, String.class, Long.TYPE, Long.TYPE));
      methods.put(
          Short.valueOf(JGroupsMethodCallDispatcher.METHOD_ID_PING),
          oClass.getMethod(METHOD_NAME_PING, String.class, Long.TYPE, Long.TYPE));
      methods.put(
          Short.valueOf(JGroupsMethodCallDispatcher.METHOD_ID_BYE),
          oClass.getMethod(METHOD_NAME_BYE, String.class, Long.TYPE, Long.TYPE));

    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Cannot gather methods", e);
    }
  }

  @Override
  public Method findMethod(final short id) {
    return methods.get(Short.valueOf(id));
  }
}
