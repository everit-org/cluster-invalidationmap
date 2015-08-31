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
