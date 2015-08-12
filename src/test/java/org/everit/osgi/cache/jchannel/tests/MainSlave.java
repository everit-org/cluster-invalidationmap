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
package org.everit.osgi.cache.jchannel.tests;

import org.everit.osgi.cache.jchannel.InvalidateSkipListMap;
import org.jgroups.Channel;
import org.jgroups.JChannel;

public class MainSlave {

  public static void main(String[] args) throws Exception {

    Channel channel = new JChannel("udp.xml");
    channel.connect("map");
    InvalidateSkipListMap<String,String> map = new InvalidateSkipListMap<>(channel);
    map.start(1000);

    map.put("b", "xxx");
    map.put("valami", "valami");

    while(System.in.available() == 0) {
      System.out.println(map);
      Thread.sleep(1000);
    }

    map.stop();
    channel.close();

  }
}
