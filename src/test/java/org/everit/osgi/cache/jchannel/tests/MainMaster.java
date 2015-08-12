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

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.everit.osgi.cache.jchannel.InvalidateSkipListMap;
import org.jgroups.Channel;
import org.jgroups.JChannel;

public class MainMaster {

  public static void main(String[] args) throws Exception {

    Channel channel = new JChannel("udp.xml");
    channel.connect("map");
    InvalidateSkipListMap<String,String> map = new InvalidateSkipListMap<>(channel);
    map.start(1000);

    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));

    bufferedReader.readLine();

    map.put("valami", "a");
    map.remove("b");
    map.remove("valami");
    map.clear();

    bufferedReader.readLine();

    map.stop();
    channel.close();
  }
}
