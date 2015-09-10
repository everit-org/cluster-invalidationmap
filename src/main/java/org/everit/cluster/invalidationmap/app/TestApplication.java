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
package org.everit.cluster.invalidationmap.app;

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

import org.everit.cluster.invalidationmap.InvalidationMap;
import org.everit.cluster.invalidationmap.InvalidationMapClusterFactory;
import org.everit.cluster.invalidationmap.jgroups.JGroupsInvalidationMapClusterFactory;
import org.everit.cluster.invalidationmap.jgroups.JGroupsNodeConfiguration;
import org.everit.cluster.invalidationmap.jgroups.DefaultJGroupsNodeConfiguration;
import org.jgroups.conf.ConfiguratorFactory;

/**
 * Test Application's frame.
 */
public class TestApplication {

  private static final int WIDTH_90 = 90;

  private static final int WIDTH_190 = 190;

  private static final int WIDTH_350 = 350;

  private static final int HEIGHT_20 = 20;

  private static final int HEIGHT_360 = 360;

  private static final int HEIGHT_450 = 450;

  private static final int POS_10 = 10;

  private static final int POS_20 = 20;

  private static final int POS_30 = 30;

  private static final int POS_50 = 50;

  private static final int POS_70 = 70;

  private static final int POS_100 = 100;

  private static final int POS_210 = 210;

  private static final int POS_340 = 340;

  private static final int POS_370 = 370;

  /**
   * Application entry point.
   *
   * @param args
   *          Arguments.
   * @throws Exception
   *           If error occurred.
   */

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
    JGroupsNodeConfiguration nodeConfig = new DefaultJGroupsNodeConfiguration(
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

  private final JFrame frame;

  private final JLabel lblMap;

  private final JList<Entry<String, String>> lstMap;

  private final JLabel lblKey;

  private final JTextField fldKey;

  private final JLabel lblValue;

  private final JTextField fldValue;

  private final JButton btnAdd;

  private final JButton btnRemove;

  private final JButton btnClear;

  /**
   * Initializes the frame.
   *
   * @param map
   *          The map
   * @param nodeName
   *          The node name.
   */
  public TestApplication(final InvalidationMap<String, String> map, final String nodeName) {
    this.map = map;

    frame = new JFrame("Invalidation Map Test :: " + nodeName);
    frame.setLayout(null);
    frame.setBounds(POS_20, POS_20, WIDTH_350, HEIGHT_450);
    frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    frame.addWindowListener(new WindowAdapter() {
      @Override
      public void windowClosing(final WindowEvent e) {
        scheduler.shutdownNow();
        map.stop();
      }
    });

    lblMap = new JLabel("Map content");
    lblMap.setBounds(POS_10, POS_10, WIDTH_190, HEIGHT_20);
    lblMap.setHorizontalAlignment(SwingConstants.CENTER);
    frame.getContentPane().add(lblMap);
    lstMap = new JList<>();
    lstMap.setBounds(POS_10, POS_30, WIDTH_190, HEIGHT_360);
    lstMap.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    lstMap.setBorder(BorderFactory.createLineBorder(Color.BLACK));
    frame.getContentPane().add(lstMap);

    lblKey = new JLabel("Key");
    lblKey.setBounds(POS_210, POS_10, WIDTH_90, HEIGHT_20);
    lblKey.setHorizontalAlignment(SwingConstants.CENTER);
    frame.getContentPane().add(lblKey);
    fldKey = new JTextField();
    fldKey.setBounds(POS_210, POS_30, WIDTH_90, HEIGHT_20);
    frame.getContentPane().add(fldKey);

    lblValue = new JLabel("Value");
    lblValue.setBounds(POS_210, POS_50, WIDTH_90, HEIGHT_20);
    lblValue.setHorizontalAlignment(SwingConstants.CENTER);
    frame.getContentPane().add(lblValue);
    fldValue = new JTextField();
    fldValue.setBounds(POS_210, POS_70, WIDTH_90, HEIGHT_20);
    frame.getContentPane().add(fldValue);

    btnAdd = new JButton("Add");
    btnAdd.setBounds(POS_210, POS_100, WIDTH_90, HEIGHT_20);
    btnAdd.addActionListener(this::btnAddAction);
    frame.getContentPane().add(btnAdd);

    btnRemove = new JButton("Remove");
    btnRemove.setBounds(POS_210, POS_340, WIDTH_90, HEIGHT_20);
    btnRemove.addActionListener(this::btnRemoveAction);
    frame.getContentPane().add(btnRemove);
    btnClear = new JButton("Clear");
    btnClear.setBounds(POS_210, POS_370, WIDTH_90, HEIGHT_20);
    btnClear.addActionListener(this::btnClearAction);
    frame.getContentPane().add(btnClear);

    SwingUtilities.invokeLater(() -> {
      map.start();
      frame.setVisible(true);
    });

    final int period = 100;
    scheduler.scheduleAtFixedRate(this::lstMapUpdate, 0, period, TimeUnit.MILLISECONDS);
  }

  /**
   * Button add action.
   *
   * @param event
   *          Event.
   */
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

  /**
   * Button remove action.
   *
   * @param event
   *          Action.
   */
  public void btnRemoveAction(final ActionEvent event) {
    if (lstMap.isSelectionEmpty()) {
      return;
    }
    String key = lstMap.getSelectedValue().getKey();
    map.remove(key);
  }

  /**
   * Updates map list box.
   */
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
