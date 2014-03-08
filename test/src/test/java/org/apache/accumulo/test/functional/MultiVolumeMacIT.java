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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.MultiVolumeMiniAccumuloClusterImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;

/**
 * 
 */
public class MultiVolumeMacIT extends AbstractMacIT {
  
  public MultiVolumeMiniAccumuloClusterImpl cluster;
  
  public int getNumFileSystems() {
    return 2;
  }
  
  public boolean useMiniDfs() {
    return false;
  }

  @Before
  public void setUp() throws Exception {
    MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(
        createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName()), ROOT_PASSWORD);
    cfg.setNativeLibPaths(NativeMapIT.nativeMapLocation().getAbsolutePath());
    
    cfg.setNumFilesystems(getNumFileSystems());
    cfg.useMiniDFS(useMiniDfs());
    
    Configuration coreSite = new Configuration(false);
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.TRUE.toString());
    
    cluster = new MultiVolumeMiniAccumuloClusterImpl(cfg);
    if (coreSite.size() > 0) {
      File csFile = new File(cluster.getConfig().getConfDir(), "core-site.xml");
      if (csFile.exists())
        throw new RuntimeException(csFile + " already exist");

      OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(cluster.getConfig().getConfDir(), "core-site.xml")));
      coreSite.writeXml(out);
      out.close();
    }
    cluster.start();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      try {
        cluster.stop();
      } catch (Exception e) {}
  }
  
  @Override
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return cluster.getConnector("root", ROOT_PASSWORD);
  }

  @Override
  public String rootPath() {
    return cluster.getConfig().getDir().getAbsolutePath();
  }
}
