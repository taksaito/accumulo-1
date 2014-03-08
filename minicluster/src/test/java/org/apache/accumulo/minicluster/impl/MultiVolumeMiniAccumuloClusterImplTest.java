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
package org.apache.accumulo.minicluster.impl;

import java.io.File;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * 
 */
public class MultiVolumeMiniAccumuloClusterImplTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));
  
  @Test//(timeout = 30000)
  public void testLocalFs() throws Exception {
    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(tempFolder.getRoot(), "password").initialize();
    config.setNumFilesystems(2);

    MultiVolumeMiniAccumuloClusterImpl accumulo = new MultiVolumeMiniAccumuloClusterImpl(config);
    try {
      accumulo.start();
      
      Connector c = accumulo.getConnector("root", "password");
      String instanceId = c.getInstance().getInstanceID();
      
      String[] volumes = accumulo.getClientConfig().getStringArray(Property.INSTANCE_VOLUMES.getKey());
      Assert.assertNotNull(volumes);
      Assert.assertEquals(config.getNumFilesystems(), volumes.length);
      
      String dfsDir = accumulo.getClientConfig().getString(Property.INSTANCE_DFS_DIR.getKey(), Property.INSTANCE_DFS_DIR.getDefaultValue());
      
      LocalFileSystem fs = FileSystem.getLocal(new Configuration());
      
      if (dfsDir.charAt(0) == '/') {
        dfsDir = dfsDir.substring(1);
      }
      
      for (String volume : volumes) {
        Path baseDir = new Path(volume, dfsDir);
        FileStatus[] fstat = fs.listStatus(new Path(baseDir, ServerConstants.INSTANCE_ID_DIR));
        Assert.assertEquals(fstat.length, 1);
        Assert.assertTrue(fstat[0].isFile());
        Assert.assertEquals(instanceId, fstat[0].getPath().getName());
      }
    } finally {
      accumulo.stop();
    }
  }
  
  @Test(timeout = 30000)
  public void testMiniDfs() throws Exception {
    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(tempFolder.getRoot(), "password");
    config.setNumFilesystems(2);
    config.useMiniDFS(true);

    MultiVolumeMiniAccumuloClusterImpl accumulo = new MultiVolumeMiniAccumuloClusterImpl(config);
    try {
      accumulo.start();
      
      Connector c = accumulo.getConnector("root", "password");
      String instanceId = c.getInstance().getInstanceID();
      
      String[] volumes = accumulo.getClientConfig().getStringArray(Property.INSTANCE_VOLUMES.getKey());
      Assert.assertNotNull(volumes);
      Assert.assertEquals(config.getNumFilesystems(), volumes.length);
      
      String dfsDir = accumulo.getClientConfig().getString(Property.INSTANCE_DFS_DIR.getKey());
      
      for (MiniDFSCluster miniDfs : accumulo.miniDfsClusters) {
        DistributedFileSystem dfs = miniDfs.getFileSystem();
        FileStatus[] fstat = dfs.listStatus(new Path(dfsDir, ServerConstants.INSTANCE_ID_DIR));
        Assert.assertEquals(fstat.length, 1);
        Assert.assertTrue(fstat[0].isFile());
        Assert.assertEquals(instanceId, fstat[0].getPath().getName());
      }
    } finally {
      accumulo.stop();
    }
  }

}
