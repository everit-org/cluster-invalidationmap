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
package org.everit.cluster.invalidationmap.jgroups;

import org.everit.cluster.invalidationmap.MapInvalidator;
import org.everit.cluster.invalidationmap.InvalidationMapCluster;
import org.everit.cluster.invalidationmap.InvalidationMapClusterFactory;
import org.everit.cluster.invalidationmap.jgroups.internal.JGroupsInvalidationMapCluster;

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
  public InvalidationMapCluster create(final MapInvalidator mapInvalidator) {
    return new JGroupsInvalidationMapCluster(
        mapInvalidator, componentName, nodeConfiguration.getNodeName(),
        nodeConfiguration.getProtocolConfigurator());
  }

}
