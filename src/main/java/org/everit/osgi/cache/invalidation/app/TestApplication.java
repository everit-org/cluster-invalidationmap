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
package org.everit.osgi.cache.invalidation.app;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import org.everit.osgi.cache.invalidation.InvalidationMap;
import org.everit.osgi.cache.invalidation.cluster.api.InvalidationMapClusterFactory;
import org.everit.osgi.cache.invalidation.cluster.jgroups.JGroupsInvalidationMapClusterFactory;
import org.everit.osgi.cache.invalidation.cluster.jgroups.JGroupsNodeConfiguration;
import org.jgroups.conf.ConfiguratorFactory;

public class TestApplication extends JFrame {

  private static final long serialVersionUID = 1L;

  public static void main(final String[] args) throws Exception {

    String nodeName = "";

    while ("".equals(nodeName)) {
      nodeName = JOptionPane.showInputDialog(null, "Pass the node name as first parameter!",
          "Node name is not set", JOptionPane.QUESTION_MESSAGE);
      if (nodeName == null) {
        return;
      }
    }

    // wrapped map
    Map<String, String> wrapped = new ConcurrentHashMap<>();

    // node configuration
    JGroupsNodeConfiguration nodeConfig = new JGroupsNodeConfiguration(
        nodeName, ConfiguratorFactory.getStackConfigurator("udp-map.xml"));

    // factory
    InvalidationMapClusterFactory factory = new JGroupsInvalidationMapClusterFactory()
        .componentName("blobStore")
        .configuration(nodeConfig);

    // invalidate map
    InvalidationMap<String, String> map = new InvalidationMap<>(wrapped, factory::create);

    startApplication(nodeName, map);

  }

  private static void startApplication(final String nodeName,
      final InvalidationMap<String, String> map) {
    SwingUtilities.invokeLater(() -> {
      new TestApplication(map, nodeName);
    });
  }

  private final InvalidationMap<String, String> map;

  private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);

  private final JLabel lblMap;

  private final JList<Entry<String, String>> lstMap;

  private final JLabel lblKey;

  private final JTextField fldKey;

  private final JLabel lblValue;

  private final JTextField fldValue;

  private final JButton btnAdd;

  private final JButton btnRemove;

  private final JButton btnClear;

  public TestApplication(final InvalidationMap<String, String> map, final String nodeName) {
    super("Invalidation Map Test :: " + nodeName);
    this.map = map;

    setLayout(null);
    setBounds(20, 20, 350, 450);
    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    addWindowListener(new WindowAdapter() {
      @Override
      public void windowClosing(final WindowEvent e) {
        scheduler.shutdownNow();
        map.stop();
      }
    });

    lblMap = new JLabel("Map content");
    lblMap.setBounds(10, 10, 190, 20);
    lblMap.setHorizontalAlignment(SwingConstants.CENTER);
    getContentPane().add(lblMap);
    lstMap = new JList<>();
    lstMap.setBounds(10, 30, 190, 360);
    lstMap.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    lstMap.setBorder(BorderFactory.createLineBorder(Color.BLACK));
    getContentPane().add(lstMap);

    lblKey = new JLabel("Key");
    lblKey.setBounds(210, 10, 90, 20);
    lblKey.setHorizontalAlignment(SwingConstants.CENTER);
    getContentPane().add(lblKey);
    fldKey = new JTextField();
    fldKey.setBounds(210, 30, 90, 20);
    getContentPane().add(fldKey);

    lblValue = new JLabel("Value");
    lblValue.setBounds(210, 50, 90, 20);
    lblValue.setHorizontalAlignment(SwingConstants.CENTER);
    getContentPane().add(lblValue);
    fldValue = new JTextField();
    fldValue.setBounds(210, 70, 90, 20);
    getContentPane().add(fldValue);

    btnAdd = new JButton("Add");
    btnAdd.setBounds(210, 100, 90, 20);
    btnAdd.addActionListener(this::btnAddAction);
    getContentPane().add(btnAdd);

    btnRemove = new JButton("Remove");
    btnRemove.setBounds(210, 340, 90, 20);
    btnRemove.addActionListener(this::btnRemoveAction);
    getContentPane().add(btnRemove);
    btnClear = new JButton("Clear");
    btnClear.setBounds(210, 370, 90, 20);
    btnClear.addActionListener(this::btnClearAction);
    getContentPane().add(btnClear);

    SwingUtilities.invokeLater(() -> {
      map.start();
      TestApplication.this.setVisible(true);
    });
    scheduler.scheduleAtFixedRate(this::lstMapUpdate, 0, 100, TimeUnit.MILLISECONDS);
  }

  public void btnAddAction(final ActionEvent event) {
    String key = fldKey.getText();
    String value = fldValue.getText();
    if (key.isEmpty()) {
      return;
    }
    map.put(key, value);
  }

  public void btnClearAction(final ActionEvent event) {
    map.clear();
  }

  public void btnRemoveAction(final ActionEvent event) {
    if (lstMap.isSelectionEmpty()) {
      return;
    }
    String key = lstMap.getSelectedValue().getKey();
    map.remove(key);
  }

  public void lstMapUpdate() {
    SwingUtilities.invokeLater(() -> {

      Entry<String, String> selectedValue = lstMap.getSelectedValue();
      lstMap.clearSelection();

      Vector<Entry<String, String>> listData = new Vector<>(map.entrySet());
      lstMap.setListData(listData);

      if (selectedValue != null) {
        for (int i = 0; i < listData.size(); i++) {
          Entry<String, String> data = listData.get(i);
          if (data.getKey().equals(selectedValue.getKey())) {
            lstMap.setSelectedIndex(i);
            break;
          }
        }
      }
    });
  }

}
