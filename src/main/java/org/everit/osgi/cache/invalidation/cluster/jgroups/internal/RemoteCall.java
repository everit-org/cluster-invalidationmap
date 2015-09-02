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
package org.everit.osgi.cache.invalidation.cluster.jgroups.internal;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.jgroups.blocks.MethodLookup;

/**
 * Remote map interface. It declares the remote invokable methods.
 *
 */
public interface RemoteCall {

  /**
   * Stores the remote methods from the {@link RemoteCall} implementation. It also implements the
   * {@link MethodLookup} through the gathered methods can be asked.
   */
  class Lookup implements MethodLookup {

    /**
     * Count of the remote methods.
     */
    private static final int METHOD_COUNT = 4;

    /**
     * Name of the method {@link RemoteCall#invalidate(String, long, long, Object)}.
     */
    private static final String METHOD_NAME_INVALIDATE = "invalidate";

    /**
     * Name of the method {@link RemoteCall#invalidateAll(String, long, long)}.
     */
    private static final String METHOD_NAME_INVALIDATE_ALL = "invalidateAll";

    /**
     * Name of the method {@link RemoteCall#ping(String, long, long)}.
     */
    private static final String METHOD_NAME_PING = "ping";

    /**
     * Name of the method {@link RemoteCall#bye(String, long, long)}.
     */
    private static final String METHOD_NAME_BYE = "bye";

    /**
     * Mandatory parameter types.
     */
    @SuppressWarnings("rawtypes")
    private static final Class[] MANDATORY_PARAMTERE_TYPES = new Class[] {
        String.class, Long.TYPE, Long.TYPE
    };

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
    public Lookup(final Class<? extends RemoteCall> oClass) {

      Objects.requireNonNull(oClass, "Cannot gather methods from null");

      try {
        methods.put(
            Short.valueOf(METHOD_ID_INVALIDATE),
            oClass.getMethod(METHOD_NAME_INVALIDATE, String.class, Long.TYPE, Long.TYPE,
                Object.class));
        methods.put(
            Short.valueOf(METHOD_ID_INVALIDATE_ALL),
            oClass.getMethod(METHOD_NAME_INVALIDATE_ALL, String.class, Long.TYPE, Long.TYPE));
        methods.put(
            Short.valueOf(METHOD_ID_PING),
            oClass.getMethod(METHOD_NAME_PING, String.class, Long.TYPE, Long.TYPE));
        methods.put(
            Short.valueOf(METHOD_ID_BYE),
            oClass.getMethod(METHOD_NAME_BYE, String.class, Long.TYPE, Long.TYPE));

        checkGatheredMethods();

      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Cannot gather methods", e);
      }
    }

    /**
     * Checks whether all of the method are declared by {@link #RemoteCall} are gathered.
     *
     * @return Method check result.
     */
    private void checkGatheredMethods() {
      Method[] methodsDeclared = RemoteCall.class.getMethods();
      Collection<Method> methodsGathered = methods.values();
      if (methodsDeclared.length != methodsGathered.size()) {
        throw new RuntimeException(
            "Method gather problem! The count of the declared and gathered methods are different.");
      }
      int found = methods.size();
      for (Method d : methodsDeclared) {

        Class<?>[] dParameterTypes = d.getParameterTypes();

        if (dParameterTypes.length < MANDATORY_PARAMETER_COUNT
            || !Arrays.deepEquals(Arrays.copyOf(dParameterTypes, MANDATORY_PARAMETER_COUNT),
                MANDATORY_PARAMTERE_TYPES)) {
          throw new RuntimeException(
              "Method gather problem! Method " + d.getName()
                  + " must declare the mandatory parameters as the first thre parameters: "
                  + Arrays.toString(MANDATORY_PARAMTERE_TYPES));
        }

        for (Method g : methodsGathered) {
          if (d.getName().equals(g.getName())
              && Arrays.deepEquals(dParameterTypes, g.getParameterTypes())) {
            found--;
            break;
          }
        }
      }
      if (found != 0) {
        throw new RuntimeException(
            "Method gather problem! Methods + " + Arrays.toString(RemoteCall.class.getMethods())
                + " were declared but methods " + methods.values() + " were gathered");

      }
    }

    @Override
    public Method findMethod(final short id) {
      return methods.get(Short.valueOf(id));
    }
  }

  /**
   * ID of method {@link #bye(String, long, long)}.
   */
  short METHOD_ID_BYE = 100;

  /**
   * ID of method {@link #ping(String, long, long)}.
   */
  short METHOD_ID_PING = 101;

  /**
   * ID of the method {@link #invalidate(String, long, long, Object)}.
   */
  short METHOD_ID_INVALIDATE = 102;

  /**
   * ID of the method {@link #invalidateAll(String, long, long)}.
   */
  short METHOD_ID_INVALIDATE_ALL = 103;

  /**
   * Mandatory parameter count.
   */
  int MANDATORY_PARAMETER_COUNT = Lookup.MANDATORY_PARAMTERE_TYPES.length;

  /**
   * Sends bye message. It must be sent before closing the channel.
   *
   * @param nodeName
   *          The name of the sender node.
   * @param startTimeNanos
   *          The start time of the node in nanoseconds.
   * @param gotMessageNumber
   *          The incremented message number.
   */
  void bye(String nodeName, long startTimeNanos, long gotMessageNumber);

  /**
   * Invalidates a key.
   *
   * @param nodeName
   *          The name of the sender node.
   * @param startTimeNanos
   *          The start time of the node in nanoseconds.
   * @param gotMessageNumber
   *          The incremented message number.
   * @param key
   *          The key.
   */
  void invalidate(String nodeName, long startTimeNanos, long gotMessageNumber, Object key);

  /**
   * Invalidate the whole map.
   *
   * @param nodeName
   *          The name of the sender node.
   * @param startTimeNanos
   *          The start time of the node in nanoseconds.
   * @param gotMessageNumber
   *          The incremented message number.
   */
  void invalidateAll(String nodeName, long startTimeNanos, long gotMessageNumber);

  /**
   * Sends ping message.
   *
   * @param nodeName
   *          The name of the sender node.
   * @param startTimeNanos
   *          The start time of the node in nanoseconds.
   * @param gotMessageNumber
   *          The previously sent message number.
   */
  void ping(String nodeName, long startTimeNanos, long gotMessageNumber);

}
