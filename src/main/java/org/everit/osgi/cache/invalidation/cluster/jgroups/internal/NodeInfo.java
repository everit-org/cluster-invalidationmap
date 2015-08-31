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

import java.io.Serializable;

import javax.annotation.Generated;

/**
 * Information about a node.
 */
public class NodeInfo implements Serializable {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = 5454223167970587690L;

  /**
   * Name of the node.
   */
  public String name;

  /**
   * Start time stamp in nanoseconds.
   */
  public long startTimeNanos;

  /**
   * Creates the instance with the given name.
   *
   * @param name
   *          The name of the node.
   */
  public NodeInfo(final String name) {
    super();
    this.name = name;
  }

  @Generated("eclipse")
  @Override
  public String toString() {
    return "MemberInfo [name=" + name + ", startTimeNanos=" + startTimeNanos + "]";
  }

}
