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
package org.everit.osgi.cache.invalidate.tests;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.everit.osgi.cache.invalidation.InvalidationMap;
import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapClusterFactory;
import org.everit.osgi.cache.invalidation.cluster.jgroups.JGroupsInvalidationMapClusterFactory;
import org.everit.osgi.cache.invalidation.cluster.jgroups.JGroupsNodeConfiguration;
import org.jgroups.conf.ConfiguratorFactory;

public class MainMaster {

  public static void main(String[] args) throws Exception {

    // wrapped map
    Map<String, String> wrapped = new ConcurrentHashMap<>();

    // node configuration
    JGroupsNodeConfiguration nodeConfig = new JGroupsNodeConfiguration(
        "node-master", ConfiguratorFactory.getStackConfigurator("udp-map.xml"));

    // factory
    InvalidationMapClusterFactory factory = new JGroupsInvalidationMapClusterFactory()
        .componentName("blobStore")
        .configuration(nodeConfig);

    // invalidate map
    InvalidationMap<String, String> map = new InvalidationMap<>(wrapped, factory::create);

    try {
      map.start(1000);
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
      bufferedReader.readLine();
      map.put("valami", "a");
      map.remove("b");
      map.remove("valami");
      map.clear();
      bufferedReader.readLine();
    } finally {
      map.stop();
    }
  }
}
