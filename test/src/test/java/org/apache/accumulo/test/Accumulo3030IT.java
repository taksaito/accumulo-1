/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class Accumulo3030IT extends ConfigurableMacIT {
  
  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Test(timeout = 60 * 1000)
  public void test() throws Exception {
    // make a table
    final String tableName = getUniqueNames(1)[0];
    final Connector conn = getConnector();
    conn.tableOperations().create(tableName);
    // make the world's slowest scanner
    final Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    final IteratorSetting cfg = new IteratorSetting(100, SlowIterator.class);
    SlowIterator.setSeekSleepTime(cfg, 99999*1000);
    scanner.addScanIterator(cfg);
    // create a thread to interrupt the slow scan
    final Thread scanThread = Thread.currentThread();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          // ensure the scan is running: not perfect, the metadata tables could be scanned, too.
          String tserver = conn.instanceOperations().getTabletServers().iterator().next();
          while (conn.instanceOperations().getActiveScans(tserver).size() < 1) {
            UtilWaitThread.sleep(1000);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        // BAM!
        scanThread.interrupt();
      }
    };
    thread.start();
    try {
      // Use the scanner, expect problems
      for (@SuppressWarnings("unused") Entry<Key,Value> entry : scanner) {
      }
      Assert.fail("Scan should not succeed");
    } catch (Exception ex) {
    } finally {
      thread.join();
    }
  }
  
}
