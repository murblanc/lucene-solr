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

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.BadVersionException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.DistributedClusterChangeUpdater;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.makeMap;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

/**
 * This class contains code used for handling Collection API calls. Some of the code here originates in {@link OverseerCollectionMessageHandler}
 * to make a clearer separation between the Overseer based handling of Collection API calls and the distributed handling of
 * Collection API calls (see org.apache.solr.handler.admin.CollectionsHandler.invokeAction()).
 */
public class CollectionHandlingUtils {
  public static final boolean CREATE_NODE_SET_SHUFFLE_DEFAULT = true;
  public static final String CREATE_NODE_SET_SHUFFLE = CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM;
  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";
  public static final String CREATE_NODE_SET = CollectionAdminParams.CREATE_NODE_SET_PARAM;
  public static final String NUM_SLICES = "numShards";
  public static final String ROUTER = "router";
  public static final String SHARDS_PROP = "shards";
  public static final String REQUESTID = "requestid";
  public static final String ONLY_IF_DOWN = "onlyIfDown";
  public static final String SHARD_UNIQUE = "shardUnique";
  public static final String ONLY_ACTIVE_NODES = "onlyactivenodes";

  public static final Map<String, Object> COLLECTION_PROPS_AND_DEFAULTS = Collections.unmodifiableMap(makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      ZkStateReader.REPLICATION_FACTOR, "1",
      ZkStateReader.NRT_REPLICAS, "1",
      ZkStateReader.TLOG_REPLICAS, "0",
      DocCollection.PER_REPLICA_STATE, null,
      ZkStateReader.PULL_REPLICAS, "0"));

  protected static final Random RANDOM;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }

  // TODO no member variables. Where do we build this class? Maybe get rid of all static? Or not build it and make everything static?

  static final String SKIP_CREATE_REPLICA_IN_CLUSTER_STATE = "skipCreateReplicaInClusterState";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static Map<String, Replica> waitToSeeReplicasInState(ZkStateReader zkStateReader, TimeSource timeSource, String collectionName, Collection<String> coreNames) throws InterruptedException {
    assert coreNames.size() > 0;
    Map<String, Replica> result = new HashMap<>();
    TimeOut timeout = new TimeOut(Integer.getInteger("solr.waitToSeeReplicasInStateTimeoutSeconds", 120), TimeUnit.SECONDS, timeSource); // could be a big cluster
    while (true) {
      DocCollection coll = zkStateReader.getClusterState().getCollection(collectionName);
      for (String coreName : coreNames) {
        if (result.containsKey(coreName)) continue;
        for (Slice slice : coll.getSlices()) {
          for (Replica replica : slice.getReplicas()) {
            if (coreName.equals(replica.getStr(ZkStateReader.CORE_NAME_PROP))) {
              result.put(coreName, replica);
              break;
            }
          }
        }
      }

      if (result.size() == coreNames.size()) {
        return result;
      } else {
        log.debug("Expecting {} cores but found {}", coreNames, result);
      }
      if (timeout.hasTimedOut()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timed out waiting to see all replicas: " + coreNames + " in cluster state. Last state: " + coll);
      }

      Thread.sleep(100);
    }
  }

  static void validateConfigOrThrowSolrException(SolrCloudManager cloudManager, String configName) throws IOException, KeeperException, InterruptedException {
    boolean isValid = cloudManager.getDistribStateManager().hasData(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName);
    if (!isValid) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not find the specified config set: " + configName);
    }
  }


  static void processResponse(NamedList<Object> results, ShardResponse srsp, Set<String> okayExceptions) {
    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(results, e, nodeName, solrResponse, shard, okayExceptions);
  }

  @SuppressWarnings("deprecation")
  static void processResponse(NamedList<Object> results, Throwable e, String nodeName, SolrResponse solrResponse, String shard, Set<String> okayExceptions) {
    String rootThrowable = null;
    if (e instanceof BaseHttpSolrClient.RemoteSolrException) {
      rootThrowable = ((BaseHttpSolrClient.RemoteSolrException) e).getRootThrowable();
    }

    if (e != null && (rootThrowable == null || !okayExceptions.contains(rootThrowable))) {
      log.error("Error from shard: {}", shard, e);
      addFailure(results, nodeName, e.getClass().getName() + ":" + e.getMessage());
    } else {
      addSuccess(results, nodeName, solrResponse.getResponse());
    }
  }

  @SuppressWarnings("unchecked")
  private static void addFailure(NamedList<Object> results, String key, Object value) {
    SimpleOrderedMap<Object> failure = (SimpleOrderedMap<Object>) results.get("failure");
    if (failure == null) {
      failure = new SimpleOrderedMap<>();
      results.add("failure", failure);
    }
    failure.add(key, value);
  }

  @SuppressWarnings("unchecked")
  private static void addSuccess(NamedList<Object> results, String key, Object value) {
    SimpleOrderedMap<Object> success = (SimpleOrderedMap<Object>) results.get("success");
    if (success == null) {
      success = new SimpleOrderedMap<>();
      results.add("success", success);
    }
    success.add(key, value);
  }

  private static NamedList<Object> waitForCoreAdminAsyncCallToComplete(ShardHandlerFactory shardHandlerFactory, String adminPath, ZkStateReader zkStateReader, String nodeName, String requestId) {
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTSTATUS.toString());
    params.set(CoreAdminParams.REQUESTID, requestId);
    int counter = 0;
    ShardRequest sreq;
    do {
      sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          NamedList<Object> results = new NamedList<>();
          processResponse(results, srsp, Collections.emptySet());
          if (srsp.getSolrResponse().getResponse() == null) {
            NamedList<Object> response = new NamedList<>();
            response.add("STATUS", "failed");
            return response;
          }

          String r = (String) srsp.getSolrResponse().getResponse().get("STATUS");
          if (r.equals("running")) {
            log.debug("The task is still RUNNING, continuing to wait.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            continue;

          } else if (r.equals("completed")) {
            log.debug("The task is COMPLETED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("failed")) {
            // TODO: Improve this. Get more information.
            log.debug("The task is FAILED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("notfound")) {
            log.debug("The task is notfound, retry");
            if (counter++ < 5) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
              break;
            }
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid status request for requestId: " + requestId + "" + srsp.getSolrResponse().getResponse().get("STATUS") +
                "retried " + counter + "times");
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid status request " + srsp.getSolrResponse().getResponse().get("STATUS"));
          }
        }
      } while (srsp != null);
    } while(true);
  }


  public static ShardRequestTracker syncRequestTracker(CollectionCommandContext ccc) {
    return asyncRequestTracker(null, ccc);
  }

  public static ShardRequestTracker asyncRequestTracker(String asyncId, CollectionCommandContext ccc) {
    return new ShardRequestTracker(asyncId, ccc.getAdminPath(), ccc.getZkStateReader(), ccc.getShardHandler().getShardHandlerFactory());
  }


  public static class ShardRequestTracker {
    /*
     * backward compatibility reasons, add the response with the async ID as top level.
     * This can be removed in Solr 9
     */
    @Deprecated
    static boolean INCLUDE_TOP_LEVEL_RESPONSE = true;

    private final String asyncId;
    private final String adminPath;
    private final ZkStateReader zkStateReader;
    private final ShardHandlerFactory shardHandlerFactory;
    private final NamedList<String> shardAsyncIdByNode = new NamedList<String>();

    public ShardRequestTracker(String asyncId, String adminPath, ZkStateReader zkStateReader, ShardHandlerFactory shardHandlerFactory) {
      this.asyncId = asyncId;
      this.adminPath = adminPath;
      this.zkStateReader = zkStateReader;
      this.shardHandlerFactory = shardHandlerFactory;
    }

    /**
     * Send request to all replicas of a slice
     * @return List of replicas which is not live for receiving the request
     */
    public List<Replica> sliceCmd(ClusterState clusterState, ModifiableSolrParams params, Replica.State stateMatcher,
                                  Slice slice, ShardHandler shardHandler) {
      List<Replica> notLiveReplicas = new ArrayList<>();
      for (Replica replica : slice.getReplicas()) {
        if ((stateMatcher == null || Replica.State.getState(replica.getStr(ZkStateReader.STATE_PROP)) == stateMatcher)) {
          if (clusterState.liveNodesContain(replica.getStr(ZkStateReader.NODE_NAME_PROP))) {
            // For thread safety, only simple clone the ModifiableSolrParams
            ModifiableSolrParams cloneParams = new ModifiableSolrParams();
            cloneParams.add(params);
            cloneParams.set(CoreAdminParams.CORE, replica.getStr(ZkStateReader.CORE_NAME_PROP));

            sendShardRequest(replica.getStr(ZkStateReader.NODE_NAME_PROP), cloneParams, shardHandler);
          } else {
            notLiveReplicas.add(replica);
          }
        }
      }
      return notLiveReplicas;
    }

    public void sendShardRequest(String nodeName, ModifiableSolrParams params,
        ShardHandler shardHandler) {
      sendShardRequest(nodeName, params, shardHandler, adminPath, zkStateReader);
    }

    public void sendShardRequest(String nodeName, ModifiableSolrParams params, ShardHandler shardHandler,
        String adminPath, ZkStateReader zkStateReader) {
      if (asyncId != null) {
        String coreAdminAsyncId = asyncId + Math.abs(System.nanoTime());
        params.set(ASYNC, coreAdminAsyncId);
        track(nodeName, coreAdminAsyncId);
      }

      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.nodeName = nodeName;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);
    }

    void processResponses(NamedList<Object> results, ShardHandler shardHandler, boolean abortOnError, String msgOnError) {
      processResponses(results, shardHandler, abortOnError, msgOnError, Collections.emptySet());
    }

    void processResponses(NamedList<Object> results, ShardHandler shardHandler, boolean abortOnError, String msgOnError,
        Set<String> okayExceptions) {
      // Processes all shard responses
      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          processResponse(results, srsp, okayExceptions);
          Throwable exception = srsp.getException();
          if (abortOnError && exception != null) {
            // drain pending requests
            while (srsp != null) {
              srsp = shardHandler.takeCompletedOrError();
            }
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msgOnError, exception);
          }
        }
      } while (srsp != null);

      // If request is async wait for the core admin to complete before returning
      if (asyncId != null) {
        waitForAsyncCallsToComplete(results); // TODO: Shouldn't we abort with msgOnError exception when failure?
        shardAsyncIdByNode.clear();
      }
    }

    private void waitForAsyncCallsToComplete(NamedList<Object> results) {
      for (Map.Entry<String,String> nodeToAsync:shardAsyncIdByNode) {
        final String node = nodeToAsync.getKey();
        final String shardAsyncId = nodeToAsync.getValue();
        log.debug("I am Waiting for :{}/{}", node, shardAsyncId);
        NamedList<Object> reqResult = waitForCoreAdminAsyncCallToComplete(shardHandlerFactory, adminPath, zkStateReader, node, shardAsyncId);
        if (INCLUDE_TOP_LEVEL_RESPONSE) {
          results.add(shardAsyncId, reqResult);
        }
        if ("failed".equalsIgnoreCase(((String)reqResult.get("STATUS")))) {
          log.error("Error from shard {}: {}", node,  reqResult);
          addFailure(results, node, reqResult);
        } else {
          addSuccess(results, node, reqResult);
        }
      }
    }

    /** @deprecated consider to make it private after {@link CreateCollectionCmd} refactoring*/
    @Deprecated void track(String nodeName, String coreAdminAsyncId) {
      shardAsyncIdByNode.add(nodeName, coreAdminAsyncId);
    }
  }

  /**
   * Send request to all replicas of a collection
   * @return List of replicas which is not live for receiving the request
   */
  static List<Replica> collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
                                     NamedList<Object> results, Replica.State stateMatcher, String asyncId, Set<String> okayExceptions,
                              CollectionCommandContext ccc, ClusterState clusterState) {
    log.info("Executing Collection Cmd={}, asyncId={}", params, asyncId);
    String collectionName = message.getStr(NAME);
    ShardHandler shardHandler = ccc.getShardHandler();
    DocCollection coll = clusterState.getCollection(collectionName);
    List<Replica> notLivesReplicas = new ArrayList<>();
    final CollectionHandlingUtils.ShardRequestTracker shardRequestTracker = asyncRequestTracker(asyncId, ccc);
    for (Slice slice : coll.getSlices()) {
      notLivesReplicas.addAll(shardRequestTracker.sliceCmd(clusterState, params, stateMatcher, slice, shardHandler));
    }

    shardRequestTracker.processResponses(results, shardHandler, false, null, okayExceptions);
    return notLivesReplicas;
  }

  /**
   * This doesn't validate the config (path) itself and is just responsible for creating the confNode.
   * That check should be done before the config node is created.
   */
  public static void createConfNode(DistribStateManager stateManager, String configName, String coll) throws IOException, AlreadyExistsException, BadVersionException, KeeperException, InterruptedException {

    if (configName != null) {
      String collDir = ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll;
      log.debug("creating collections conf node {} ", collDir);
      byte[] data = Utils.toJSON(makeMap(ZkController.CONFIGNAME_PROP, configName));
      if (stateManager.hasData(collDir)) {
        stateManager.setData(collDir, data, -1);
      } else {
        stateManager.makePath(collDir, data, CreateMode.PERSISTENT, false);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"Unable to get config name");
    }
  }

  static void checkRequired(ZkNodeProps message, String... props) {
    for (String prop : props) {
      if(message.get(prop) == null){
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, StrUtils.join(Arrays.asList(props),',') +" are required params" );
      }
    }

  }

  public static String waitForCoreNodeName(String collectionName, String msgNodeName, String msgCore, ZkStateReader zkStateReader) {
    int retryCount = 320;
    while (retryCount-- > 0) {
      final DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
      if (docCollection != null && docCollection.getSlicesMap() != null) {
        Map<String,Slice> slicesMap = docCollection.getSlicesMap();
        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {
            // TODO: for really large clusters, we could 'index' on this

            String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);

            if (nodeName.equals(msgNodeName) && core.equals(msgCore)) {
              return replica.getName();
            }
          }
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not find coreNodeName");
  }

  static ClusterState waitForNewShard(String collectionName, String sliceName, ZkStateReader zkStateReader) throws KeeperException, InterruptedException {
    log.debug("Waiting for slice {} of collection {} to be available", sliceName, collectionName);
    RTimer timer = new RTimer();
    int retryCount = 320;
    while (retryCount-- > 0) {
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection collection = clusterState.getCollection(collectionName);

      if (collection == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unable to find collection: " + collectionName + " in clusterstate");
      }
      Slice slice = collection.getSlice(sliceName);
      if (slice != null) {
        if (log.isDebugEnabled()) {
          log.debug("Waited for {}ms for slice {} of collection {} to be available",
              timer.getTime(), sliceName, collectionName);
        }
        return clusterState;
      }
      Thread.sleep(1000);
    }
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
        "Could not find new slice " + sliceName + " in collection " + collectionName
            + " even after waiting for " + timer.getTime() + "ms"
    );
  }

  @SuppressWarnings({"unchecked"})
  static void commit(@SuppressWarnings({"rawtypes"})NamedList results, String slice, Replica parentShardLeader) {
    log.debug("Calling soft commit to make sub shard updates visible");
    String coreUrl = new ZkCoreNodeProps(parentShardLeader).getCoreUrl();
    // HttpShardHandler is hard coded to send a QueryRequest hence we go direct
    // and we force open a searcher so that we have documents to show upon switching states
    UpdateResponse updateResponse = null;
    try {
      updateResponse = softCommit(coreUrl);
      CollectionHandlingUtils.processResponse(results, null, coreUrl, updateResponse, slice, Collections.emptySet());
    } catch (Exception e) {
      CollectionHandlingUtils.processResponse(results, e, coreUrl, updateResponse, slice, Collections.emptySet());
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to call distrib softCommit on: " + coreUrl, e);
    }
  }


  static UpdateResponse softCommit(String url) throws SolrServerException, IOException {

    try (HttpSolrClient client = new HttpSolrClient.Builder(url)
        .withConnectionTimeout(30000)
        .withSocketTimeout(120000)
        .build()) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParams(new ModifiableSolrParams());
      ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true, true);
      return ureq.process(client);
    }
  }

  static void addPropertyParams(ZkNodeProps message, ModifiableSolrParams params) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(CollectionAdminParams.PROPERTY_PREFIX)) {
        params.set(key, message.getStr(key));
      }
    }
  }

  static void addPropertyParams(ZkNodeProps message, Map<String, Object> map) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(CollectionAdminParams.PROPERTY_PREFIX)) {
        map.put(key, message.getStr(key));
      }
    }
  }

  static void cleanupCollection(String collectionName, @SuppressWarnings({"rawtypes"})NamedList results, CollectionCommandContext ccc) throws Exception {
    log.error("Cleaning up collection [{}].", collectionName);
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, DELETE.toLower(),
        NAME, collectionName);
    new DeleteCollectionCmd(ccc).call(ccc.getZkStateReader().getClusterState(), new ZkNodeProps(props), results);
  }

  static boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms, ZkStateReader zkStateReader) throws InterruptedException {
    try {
      zkStateReader.waitForState(collectionName, timeoutms, TimeUnit.MILLISECONDS, (c) -> {
        if (c == null)
          return true;
        Slice slice = c.getSlice(shard);
        if(slice == null || slice.getReplica(replicaName) == null) {
          return true;
        }
        return false;
      });
    } catch (TimeoutException e) {
      return false;
    }

    return true;
  }

  static void deleteCoreNode(String collectionName, String replicaName, Replica replica, String core, CollectionCommandContext ccc) throws Exception {
    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
        ZkStateReader.CORE_NAME_PROP, core,
        ZkStateReader.NODE_NAME_PROP, replica.getNodeName(),
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.CORE_NODE_NAME_PROP, replicaName);
    if (ccc.getDistributedClusterChangeUpdater().isDistributedStateChange()) {
      ccc.getDistributedClusterChangeUpdater().doSingleStateUpdate(DistributedClusterChangeUpdater.MutatingCommand.SliceRemoveReplica, m,
          ccc.getSolrCloudManager(), ccc.getZkStateReader());
    } else {
      ccc.offerStateUpdate(Utils.toJSON(m));
    }
  }

  static void checkResults(String label, NamedList<Object> results, boolean failureIsFatal) throws SolrException {
    Object failure = results.get("failure");
    if (failure == null) {
      failure = results.get("error");
    }
    if (failure != null) {
      String msg = "Error: " + label + ": " + Utils.toJSONString(results);
      if (failureIsFatal) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
      } else {
        log.error(msg);
      }
    }
  }


  /* ---------------------------------------------------------------------------------------------------------------- */

  /**
   * Interface for all Collection API commands.
   */
  protected interface Cmd {
    void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception;
  }

  static public class MockOperationCmd implements Cmd {
    @SuppressForbidden(reason = "Needs currentTimeMillis for mock requests")
    @SuppressWarnings({"unchecked"})
    public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws InterruptedException {
      //only for test purposes
      Thread.sleep(message.getInt("sleep", 1));
      if (log.isInfoEnabled()) {
        log.info("MOCK_TASK_EXECUTED time {} data {}", System.currentTimeMillis(), Utils.toJSONString(message));
      }
      results.add("MOCK_FINISHED", System.currentTimeMillis());
    }
  }

  static public class ReloadCollectionCmd implements CollectionHandlingUtils.Cmd {
    private final CollectionCommandContext ccc;

    public ReloadCollectionCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    @SuppressWarnings({"unchecked"})
    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.RELOAD.toString());

      String asyncId = message.getStr(ASYNC);
      collectionCmd(message, params, results, Replica.State.ACTIVE, asyncId, Collections.emptySet(), ccc, clusterState);
    }
  }


  static public class ModifyCollectionCmd implements CollectionHandlingUtils.Cmd {
    private final CollectionCommandContext ccc;

    public ModifyCollectionCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {

      final String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
      //the rest of the processing is based on writing cluster state properties
      //remove the property here to avoid any errors down the pipeline due to this property appearing
      String configName = (String) message.getProperties().remove(CollectionAdminParams.COLL_CONF);

      if (configName != null) {
        CollectionHandlingUtils.validateConfigOrThrowSolrException(ccc.getSolrCloudManager(), configName);

        createConfNode(ccc.getSolrCloudManager().getDistribStateManager(), configName, collectionName);
        new ReloadCollectionCmd(ccc).call(null, new ZkNodeProps(NAME, collectionName), results);
      }

      if (ccc.getDistributedClusterChangeUpdater().isDistributedStateChange()) {
        // Apply the state update right away. The wait will still be useful for the change to be visible in the local cluster state (watchers have fired).
        ccc.getDistributedClusterChangeUpdater().doSingleStateUpdate(DistributedClusterChangeUpdater.MutatingCommand.CollectionModifyCollection, message,
            ccc.getSolrCloudManager(), ccc.getZkStateReader());
      } else {
        ccc.offerStateUpdate(Utils.toJSON(message));
      }

      TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, ccc.getSolrCloudManager().getTimeSource());
      boolean areChangesVisible = true;
      while (!timeout.hasTimedOut()) {
        DocCollection collection = ccc.getSolrCloudManager().getClusterStateProvider().getClusterState().getCollection(collectionName);
        areChangesVisible = true;
        for (Map.Entry<String, Object> updateEntry : message.getProperties().entrySet()) {
          String updateKey = updateEntry.getKey();

          if (!updateKey.equals(ZkStateReader.COLLECTION_PROP)
              && !updateKey.equals(Overseer.QUEUE_OPERATION)
              && updateEntry.getValue() != null // handled below in a separate conditional
              && !updateEntry.getValue().equals(collection.get(updateKey))) {
            areChangesVisible = false;
            break;
          }

          if (updateEntry.getValue() == null && collection.containsKey(updateKey)) {
            areChangesVisible = false;
            break;
          }
        }
        if (areChangesVisible) break;
        timeout.sleep(100);
      }

      if (!areChangesVisible)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not modify collection " + message);

      // if switching to/from read-only mode reload the collection
      if (message.keySet().contains(ZkStateReader.READ_ONLY)) {
        new ReloadCollectionCmd(ccc).call(null, new ZkNodeProps(NAME, collectionName), results);
      }
    }
  }



  static public class AddReplicaPropCmd implements CollectionHandlingUtils.Cmd {
    private final CollectionCommandContext ccc;

    public AddReplicaPropCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    @SuppressWarnings("unchecked")
    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
        throws Exception {
      checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP, PROPERTY_VALUE_PROP);
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
      propMap.putAll(message.getProperties());
      ZkNodeProps m = new ZkNodeProps(propMap);
      if (ccc.getDistributedClusterChangeUpdater().isDistributedStateChange()) {
        ccc.getDistributedClusterChangeUpdater().doSingleStateUpdate(DistributedClusterChangeUpdater.MutatingCommand.ReplicaAddReplicaProperty, m,
            ccc.getSolrCloudManager(), ccc.getZkStateReader());
      } else {
        ccc.offerStateUpdate(Utils.toJSON(m));
      }
    }

  }

  static public class RebalanceLeadersCmd implements CollectionHandlingUtils.Cmd {
    private final CollectionCommandContext ccc;

    public RebalanceLeadersCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    @SuppressWarnings("unchecked")
    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results)
        throws Exception {
      checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, CORE_NAME_PROP, ELECTION_NODE_PROP,
          CORE_NODE_NAME_PROP, NODE_NAME_PROP, REJOIN_AT_HEAD_PROP);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(COLLECTION_PROP, message.getStr(COLLECTION_PROP));
      params.set(SHARD_ID_PROP, message.getStr(SHARD_ID_PROP));
      params.set(REJOIN_AT_HEAD_PROP, message.getStr(REJOIN_AT_HEAD_PROP));
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REJOINLEADERELECTION.toString());
      params.set(CORE_NAME_PROP, message.getStr(CORE_NAME_PROP));
      params.set(CORE_NODE_NAME_PROP, message.getStr(CORE_NODE_NAME_PROP));
      params.set(ELECTION_NODE_PROP, message.getStr(ELECTION_NODE_PROP));
      params.set(NODE_NAME_PROP, message.getStr(NODE_NAME_PROP));

      String baseUrl = UrlScheme.INSTANCE.getBaseUrlForNodeName(message.getStr(NODE_NAME_PROP));
      ShardRequest sreq = new ShardRequest();
      sreq.nodeName = message.getStr(ZkStateReader.CORE_NAME_PROP);
      // yes, they must use same admin handler path everywhere...
      params.set("qt", ccc.getAdminPath());
      sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
      sreq.shards = new String[]{baseUrl};
      sreq.actualShards = sreq.shards;
      sreq.params = params;
      ShardHandler shardHandler = ccc.getShardHandler();
      shardHandler.submit(sreq, baseUrl, sreq.params);
    }
  }



  static public class DeleteReplicaPropCmd implements CollectionHandlingUtils.Cmd {
    private final CollectionCommandContext ccc;

    public DeleteReplicaPropCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results)
        throws Exception {
      checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP);
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, DELETEREPLICAPROP.toLower());
      propMap.putAll(message.getProperties());
      ZkNodeProps m = new ZkNodeProps(propMap);
      if (ccc.getDistributedClusterChangeUpdater().isDistributedStateChange()) {
        ccc.getDistributedClusterChangeUpdater().doSingleStateUpdate(DistributedClusterChangeUpdater.MutatingCommand.ReplicaDeleteReplicaProperty, m,
            ccc.getSolrCloudManager(), ccc.getZkStateReader());
      } else {
        ccc.offerStateUpdate(Utils.toJSON(m));
      }
    }
  }

  static public class BalanceShardsUniqueCmd implements CollectionHandlingUtils.Cmd {
    private final CollectionCommandContext ccc;

    public BalanceShardsUniqueCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
      if (StringUtils.isBlank(message.getStr(COLLECTION_PROP)) || StringUtils.isBlank(message.getStr(PROPERTY_PROP))) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "The '" + COLLECTION_PROP + "' and '" + PROPERTY_PROP +
                "' parameters are required for the BALANCESHARDUNIQUE operation, no action taken");
      }
      Map<String, Object> m = new HashMap<>();
      m.put(Overseer.QUEUE_OPERATION, BALANCESHARDUNIQUE.toLower());
      m.putAll(message.getProperties());
      if (ccc.getDistributedClusterChangeUpdater().isDistributedStateChange()) {
        ccc.getDistributedClusterChangeUpdater().doSingleStateUpdate(DistributedClusterChangeUpdater.MutatingCommand.BalanceShardsUnique, new ZkNodeProps(m),
            ccc.getSolrCloudManager(), ccc.getZkStateReader());
      } else {
        ccc.offerStateUpdate(Utils.toJSON(m));
      }
    }
  }


}
