/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.client.ExternalViewReader.OFFLINE_SUFFIX;
import static org.apache.pinot.client.ExternalViewReader.REALTIME_SUFFIX;


public class ControllerBasedBrokerSelector implements BrokerSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerBasedBrokerSelector.class);

  public static final int DEFAULT_CONTROLLER_REQUEST_RETRIES = 3;
  public static final long DEFAULT_CONTROLLER_REQUEST_RETRIES_INTERVAL_IN_MILLS = 5000; // 5 seconds
  public static final long DEFAULT_CONTROLLER_REQUEST_SCHEDULE_INTERVAL_IN_MILLS = 300000; // 5 minutes

  private static final String CONTROLLER_ONLINE_TABLE_BROKERS_MAP_URL_TEMPLATE = "%s/brokers/tables?state=online";
  private static final Random RANDOM = new Random();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String[] controllerUrls;
  private final AtomicReference<Map<String, List<String>>> tableToBrokerListMapRef =
      new AtomicReference<Map<String, List<String>>>();
  private final AtomicReference<List<String>> allBrokerListRef = new AtomicReference<List<String>>();
  private final int controllerFetchRetries;
  private final long controllerFetchRetriesIntervalMills;
  private final long controllerFetchScheduleIntervalMills;

  public ControllerBasedBrokerSelector(String controllerUrl) {
    this(controllerUrl, DEFAULT_CONTROLLER_REQUEST_RETRIES, DEFAULT_CONTROLLER_REQUEST_RETRIES_INTERVAL_IN_MILLS,
        DEFAULT_CONTROLLER_REQUEST_SCHEDULE_INTERVAL_IN_MILLS);
  }

  public ControllerBasedBrokerSelector(String controllerUrl, int controllerFetchRetries,
      long controllerFetchRetriesIntervalMills, long controllerFetchScheduleIntervalMills) {
    this.controllerUrls = controllerUrl.split(",");
    this.controllerFetchRetries = controllerFetchRetries;
    this.controllerFetchRetriesIntervalMills = controllerFetchRetriesIntervalMills;
    this.controllerFetchScheduleIntervalMills = controllerFetchScheduleIntervalMills;
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(() -> refreshBroker(), this.controllerFetchScheduleIntervalMills,
        this.controllerFetchScheduleIntervalMills, TimeUnit.MILLISECONDS);
    refreshBroker();
  }

  private void refreshBroker() {
    Map<String, List<String>> tableToBrokerListMap = getTableToBrokersMap();
    if (tableToBrokerListMap == null) {
      throw new RuntimeException("Unable to fetch broker information from controller");
    }
    tableToBrokerListMapRef.set(ImmutableMap.copyOf(tableToBrokerListMap));
    Set<String> brokerSet = new HashSet<>();
    for (List<String> brokerList : tableToBrokerListMap.values()) {
      brokerSet.addAll(brokerList);
    }
    allBrokerListRef.set(ImmutableList.copyOf(brokerSet));
  }

  private Map<String, List<String>> getTableToBrokersMap() {
    int controllerIdx = RANDOM.nextInt(controllerUrls.length);
    for (int i = 0; i < controllerFetchRetries; i++) {
      try {
        String brokerTableQueryResp = IOUtils.toString(
            new URL(String.format(CONTROLLER_ONLINE_TABLE_BROKERS_MAP_URL_TEMPLATE, controllerUrls[controllerIdx])));
        Map<String, List<String>> tablesToBrokersMap = OBJECT_MAPPER.readValue(brokerTableQueryResp, Map.class);
        for (String table : tablesToBrokersMap.keySet()) {
          List<String> brokerHostPorts = new ArrayList<>();
          for (String broker : tablesToBrokersMap.get(table)) {
            brokerHostPorts.add(broker.replace("Broker_", "").replace("_", ":"));
          }
          tablesToBrokersMap.put(table, brokerHostPorts);
        }
        return tablesToBrokersMap;

      } catch (Exception e) {
        LOGGER.warn(String.format("Unable to fetch controller broker information from '%s', retry in %d millseconds",
            controllerUrls[controllerIdx], this.controllerFetchRetriesIntervalMills), e);
        controllerIdx = (controllerIdx + 1) % controllerUrls.length;
        try {
          Thread.sleep(controllerFetchRetriesIntervalMills);
        } catch (InterruptedException interruptedException) {
          // Swallow
        }
      }
    }
    LOGGER.warn(
        String.format("Failed to fetch controller broker information with %d retries", this.controllerFetchRetries));
    return null;
  }

  @Override
  public String selectBroker(String table) {
    if (table == null) {
      List<String> list = allBrokerListRef.get();
      if (list != null && !list.isEmpty()) {
        return list.get(RANDOM.nextInt(list.size()));
      } else {
        return null;
      }
    }
    String tableName = table.replace(OFFLINE_SUFFIX, "").replace(REALTIME_SUFFIX, "");
    List<String> list = tableToBrokerListMapRef.get().get(tableName);
    if (list != null && !list.isEmpty()) {
      return list.get(RANDOM.nextInt(list.size()));
    }
    return null;
  }
}
