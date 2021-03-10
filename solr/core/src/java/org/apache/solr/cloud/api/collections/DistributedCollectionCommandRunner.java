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

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.DistributedLock;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.ZkDistributedLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Class for execution Collection API commands in a distributed way, without going through Overseer and
 * {@link OverseerCollectionMessageHandler}.<p>
 *
 * This class is only called when Collection API calls are configured to be distributed, which implies cluster state
 * updates are distributed as well.
 */
public class DistributedCollectionCommandRunner {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ExecutorService distributedCollectionApiExecutorService;
  private final CoreContainer coreContainer;
  final private CollApiCmds.CommandMap commandMapper;
  private final CollectionCommandContext ccc;
  private final ApiLockingHelper apiLockingHelper;

  public DistributedCollectionCommandRunner(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;

    // TODO we should look at how everything is getting closed when the node is shutdown.
    //  But it seems that CollectionsHandler (that creates instances of this class) is not really closed, so maybe it doesn't matter?
    // With distributed Collection API execution, each node will have such an executor but given how thread pools work,
    // threads will only be created if needed (including the corePoolSize threads).
    distributedCollectionApiExecutorService = new ExecutorUtil.MDCAwareThreadPoolExecutor(5, 10, 0L, TimeUnit.MILLISECONDS,
        new SynchronousQueue<>(),
        new SolrNamedThreadFactory("DistributedCollectionCommandRunnerThreadFactory"));

    ccc = new DistributedCollectionCommandContext(this.coreContainer, this.distributedCollectionApiExecutorService);
    commandMapper = new CollApiCmds.CommandMap(ccc);

    apiLockingHelper = new ApiLockingHelper(new ZkDistributedLockFactory(ccc.getZkStateReader().getZkClient()));
  }

  /**
   * When {@link org.apache.solr.handler.admin.CollectionsHandler#invokeAction} does not enqueue to overseer queue and
   * instead calls this method, this method is expected to do the equivalent of what Overseer does in
   * {@link org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler#processMessage}.
   * <p>
   * The steps leading to that call in the Overseer execution path are (and the equivalent is done here):
   * <ul>
   * <li>{@link org.apache.solr.cloud.OverseerTaskProcessor#run()} gets the message from the ZK queue, grabs the
   * corresponding lock (Collection API calls do locking to prevent non compatible concurrent modifications of a collection),
   * marks the async id of the task as running then executes the command using an executor service</li>
   * <li>In {@link org.apache.solr.cloud.OverseerTaskProcessor}.{@code Runner.run()} (run on an executor thread) a call is made to
   * {@link org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler#processMessage} which sets the logging
   * context, calls {@link CollApiCmds.CollectionApiCommand#call}
   * TODO and anything else?</li>
   * </ul>
   */
  @SuppressWarnings("unchecked")
  public OverseerSolrResponse runApiCommand(ZkNodeProps message, CollectionParams.CollectionAction action) throws KeeperException, InterruptedException {
    final String collName = getCollectionName(message);
    final String shardId = message.getStr(SHARD_ID_PROP);
    final String replicaName = message.getStr(REPLICA_PROP);

    MDCLoggingContext.setCollection(collName);
    MDCLoggingContext.setShard(shardId);
    MDCLoggingContext.setReplica(replicaName);

    // Create locks for executing the command. This call is non blocking (not on waiting for a lock anyway)
    List<DistributedLock> locks = apiLockingHelper.getCollectionApiLocks(action.lockLevel, collName, shardId, replicaName);

    log.debug("DistributedCollectionCommandRunner.runApiCommand. About to acquire locks for action {} lock level {}. {}/{}/{}",
        action, action.lockLevel, collName, shardId, replicaName);

    // Block this thread until all required locks are acquired.
    // This thread is either a CollectionsHandler thread and a client is waiting for a response, or this thread comes from a pool (WHERE?) and is running an async task.
    for (DistributedLock lock : locks) {
      log.debug("DistributedCollectionCommandRunner.runApiCommand. About to acquire lock {}", lock);
      lock.waitUntilAcquired();
      log.debug("DistributedCollectionCommandRunner.runApiCommand. Acquired lock {}", lock);
    }

    // TODO handle async tasks... Before and after command call
    // Note async tasks will be executed in the order enqueued on a node (no guarantees for enqueues on different nodes)
    // But async vs non async task, no guarantees either. The non async task might get executed first. So when combining both
    // should document that need to wait for completion first (if order matters)

    log.debug("DistributedCollectionCommandRunner.runApiCommand. Locks acquired. Calling: {}, {}", action, message);

    @SuppressWarnings({"rawtypes"})
    NamedList results = new NamedList();
    try {
      CollApiCmds.CollectionApiCommand command = commandMapper.getActionCommand(action);
      if (command != null) {
        command.call(ccc.getSolrCloudManager().getClusterStateProvider().getClusterState(), message, results);
      } else {
        // Seeing this is a bug, no bad user data
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown operation: " + action);
      }
    } catch (Exception e) {
      // Output some error logs
      if (collName == null) {
        SolrException.log(log, "Operation " + action + " failed", e);
      } else  {
        SolrException.log(log, "Collection " + collName + ", operation " + action + " failed", e);
      }

      results.add("Operation " + action + " caused exception:", e);
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException)e).code() : -1);
      results.add("exception", nl);
    } finally {
      for (DistributedLock dl : locks) {
        dl.release();
      }
    }
    return new OverseerSolrResponse(results);
  }

  /**
   * Collection name can be found in either of two message parameters (why??). Return it from where it's defined.
   * (see also parameter {@code collectionNameParamName} of {@link org.apache.solr.cloud.DistributedClusterStateUpdater.MutatingCommand#MutatingCommand(CollectionParams.CollectionAction, String)})
   */
  public static String getCollectionName(ZkNodeProps message) {
    return message.containsKey(COLLECTION_PROP) ?
        message.getStr(COLLECTION_PROP) : message.getStr(NAME);
  }
}
