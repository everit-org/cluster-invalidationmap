package org.everit.osgi.cache.invalidation.cluster.jgroups;

import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapCallback;
import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapCluster;
import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapClusterFactory;
import org.everit.osgi.cache.invalidation.cluster.jgroups.internal.JGroupsInvalidationMapCluster;

/**
 * {@link org.jgroups.JChannel} backed {@link InvalidationMapClusterFactory} factory.
 */
public class JGroupsInvalidationMapClusterFactory implements InvalidationMapClusterFactory {

  /**
   * The name of the component.
   */
  private String componentName;

  /**
   * The configuration of the channel.
   */
  private JGroupsNodeConfiguration nodeConfiguration;

  /**
   * Updates the component name.
   *
   * @param componentName
   *          The component name.
   * @return Self.
   */
  public JGroupsInvalidationMapClusterFactory componentName(final String componentName) {
    this.componentName = componentName;
    return this;
  }

  /**
   * Updates the node configuration.
   *
   * @param nodeConfiguration
   *          The configuration of the node.
   * @return Self.
   */
  public InvalidationMapClusterFactory configuration(
      final JGroupsNodeConfiguration nodeConfiguration) {
    this.nodeConfiguration = nodeConfiguration;
    return this;
  }

  @Override
  public InvalidationMapCluster create(final InvalidationMapCallback callback) {
    return new JGroupsInvalidationMapCluster(callback, componentName, nodeConfiguration.nodeName,
        nodeConfiguration.protocolConfigurator);
  }

}
