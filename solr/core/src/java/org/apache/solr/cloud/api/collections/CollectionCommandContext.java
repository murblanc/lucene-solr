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
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Data passed to Collection and Config Set API command execution, to allow calls from either the {@link OverseerCollectionMessageHandler}
 * or distributed calls directly from the {@link org.apache.solr.handler.admin.CollectionsHandler} (need to support both
 * modes for a while until Overseer is actually removed).
 */
public interface CollectionCommandContext {
  ShardHandler getShardHandler();
  SolrCloudManager getSolrCloudManager();
  ZkStateReader getZkStateReader();
  // validateConfigOrThrowSolrException easily rewritten with solrCloudManager

  DistributedClusterChangeUpdater getDistributedClusterChangeUpdater();
  // TODO ocmh.cloudManager.getClusterStateProvider().getClusterState().hasCollection(collectionName) needs rework
  CoreContainer getCoreContainer();

  // TODO move class away from OCMH (everything static anyway)
  default OverseerCollectionMessageHandler.ShardRequestTracker asyncRequestTracker(String asyncId) {
    return new OverseerCollectionMessageHandler.ShardRequestTracker(asyncId, getAdminPath(), getZkStateReader(), getShardHandler().getShardHandlerFactory());
  }

  // ocmh.cleanupCollection can be redone with zkStateReader

  void validateConfigOrThrowSolrException(String configName) throws IOException, KeeperException, InterruptedException;

  default Map<String, Replica> waitToSeeReplicasInState(String collectionName, Collection<String> coreNames) throws InterruptedException {
    return OverseerCollectionMessageHandler.waitToSeeReplicasInState(getZkStateReader(), getSolrCloudManager().getTimeSource(), collectionName, coreNames);
  }

  /**
   * This method enables the commands to enqueue to the overseer cluster state update. This should only be used when the command
   * is running in the Overseer (and will throw an exception if called when Collection API is distributed)
   */
  default void offerStateUpdate(byte[] data) throws KeeperException, InterruptedException {
    throw new IllegalStateException("Bug! This default implementation should never be called");
  };

  /**
   * admin path passed to Overseer is a constant, including in tests. Unclear if it's needed...
   */
  default String getAdminPath() {
    return CommonParams.CORES_HANDLER_PATH;
  }

}
