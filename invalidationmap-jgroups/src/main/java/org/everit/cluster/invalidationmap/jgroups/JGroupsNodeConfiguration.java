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

import org.jgroups.conf.ProtocolStackConfigurator;

/**
 * Node configuration implementation.
 */
public class JGroupsNodeConfiguration {

  /**
   * Name of the node.
   */
  private final String nodeName;

  /**
   * The protocol configuration for the {@link org.jgroups.JChannel}.
   */
  private final ProtocolStackConfigurator protocolConfigurator;

  public JGroupsNodeConfiguration(final String nodeName,
      final ProtocolStackConfigurator protocolConfigurator) {
    this.nodeName = nodeName;
    this.protocolConfigurator = protocolConfigurator;
  }

  /**
   * Returns the name of the node in the cluster. Must be unique in the cluster.
   * @return The node name.
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Returns the protocol stack configuration for the {@link org.jgroups.JChannel}.
   *
   * @return The configuration.
   */
  public ProtocolStackConfigurator getProtocolConfigurator() {
    return protocolConfigurator;
  }

}
