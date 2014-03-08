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
package org.apache.accumulo.test.functional;

import java.io.File;
import java.util.Collection;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class MultiVolumeIT extends MultiVolumeMacIT {

  @Override
  public int getNumFileSystems() {
    return 2;
  }

  @Test
  public void testCreateWriteDeleteLocalFs() throws Exception {
    Connector c = getConnector();
    TableOperations tops = c.tableOperations();

    // This should be statistically significant (<5%) to ensure that all have files
    for (int i = 0; i < 25; i++) {
      final String tableName = testName.getMethodName() + i;
      tops.create(tableName);
      
      BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      for (int row = 0; row < 500; row++) {
        // rows
        Mutation m = new Mutation(Integer.toString(row));
        for (int col = 0; col < 50; col++) {
          m.put(Integer.toString(col), "", Integer.toString(row) + col);
        }
        
        bw.addMutation(m);
      }
      
      bw.close();
      tops.compact(tableName, null, null, true, true);
    }

    File dir = cluster.getConfig().getDir();
    File accumuloDir = new File(dir, "accumulo");
    String[] volumeDirs = accumuloDir.list();
    Assert.assertEquals(2, volumeDirs.length);
    
    for (String volume : volumeDirs) {
      File volumeDir = new File(accumuloDir, volume);
      Collection<File> rfiles = FileUtils.listFiles(volumeDir, new IOFileFilter() {

        @Override
        public boolean accept(File file) {
          if (file.isFile()) {
            return file.getName().endsWith(".rf");
          } else {
            return true;
          }
        }

        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".rf");
        }
        
      }, new IOFileFilter() {

        @Override
        public boolean accept(File file) {
          return true;
        }

        @Override
        public boolean accept(File dir, String name) {
          return true;
        }
        
      });
      
      Assert.assertTrue("Found no rfiles in " + volumeDir, rfiles.size() > 0);
    }
    
    for (int i = 0; i < 6; i++) {
      tops.delete(testName.getMethodName() + i);
    }
  }

}
