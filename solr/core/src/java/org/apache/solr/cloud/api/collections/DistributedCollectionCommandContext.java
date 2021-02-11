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

package org.apache.solr.cloud.api.collections;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.DistributedClusterChangeUpdater;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class DistributedCollectionCommandContext implements CollectionCommandContext {
  private final ShardHandler shardHandler;
  private final SolrCloudManager solrCloudManager;
  private final CoreContainer coreContainer;
  private final ZkStateReader zkStateReader;
  private final DistributedClusterChangeUpdater distributedClusterChangeUpdater;

  public DistributedCollectionCommandContext(ShardHandler shardHandler,
                                             SolrCloudManager solrCloudManager,
                                             CoreContainer coreContainer,
                                             ZkStateReader zkStateReader,
                                             DistributedClusterChangeUpdater distributedClusterChangeUpdater) {
    this.shardHandler = shardHandler;
    this.solrCloudManager = solrCloudManager;
    this.coreContainer = coreContainer;
    this.zkStateReader = zkStateReader;
    this.distributedClusterChangeUpdater = distributedClusterChangeUpdater;
  }

  @Override
  public ShardHandler getShardHandler() {
    return this.shardHandler;
  }

  @Override
  public SolrCloudManager getSolrCloudManager() {
    return this.solrCloudManager;
  }

  @Override
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  // TODO calls through here to get the cluster state should be reviewed
  public ZkStateReader getZkStateReader() {
    return this.zkStateReader;
  }

  @Override
  public void validateConfigOrThrowSolrException(String configName) throws IOException, KeeperException, InterruptedException {
    // FIXME copied the internals of OverseerCollectionMessageHandler.validateConfigOrThrowSolrException(). Need to factor this out.
    boolean isValid = solrCloudManager.getDistribStateManager().hasData(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName);
    if(!isValid) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not find the specified config set: " + configName);
    }
  }

  @Override
  public DistributedClusterChangeUpdater getDistributedClusterChangeUpdater() {
    return this.distributedClusterChangeUpdater;
  }

  @Override
  public SolrCloseable getCloseableToLatchOn() {
    return null; // TODO implement
  }

  @Override
  public ExecutorService getExecutorService() {
    return null; // TODO implement
  }

  @Override
  public boolean isDistributedCollectionAPI() {
    // If we build this instance we're running distributed.
    return true;
  }

}
