package org.everit.osgi.cache.invalidation.cluster.jgroups;

import org.jgroups.conf.ProtocolStackConfigurator;

/**
 * Node configuration for {@link org.jgroups.JChannel}.
 */
public class JGroupsNodeConfiguration {

  /**
   * Name of the node.
   */
  public final String nodeName;

  /**
   * The protocol configurator for the {@link org.jgroups.JChannel}.
   */
  public final ProtocolStackConfigurator protocolConfigurator;

  public JGroupsNodeConfiguration(final String nodeName,
      final ProtocolStackConfigurator protocolConfigurator) {
    this.nodeName = nodeName;
    this.protocolConfigurator = protocolConfigurator;
  }

}
