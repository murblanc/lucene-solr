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

package org.apache.solr.cloud;

import com.google.common.base.Preconditions;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * A distributed lock implementation using Zookeeper "directory" nodes created within the collection znode hierarchy.
 * The locks are implemented using ephemeral nodes placed below the "directory" nodes.
 *
 * @see <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks">Zookeeper lock recipe</a>
 */
public class ZkDistributedLockFactory implements DistributedLockFactory {

  static final String ZK_PATH_SEPARATOR = "/";

  private final SolrZkClient zkClient;

  public ZkDistributedLockFactory(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }


  public DistributedLock createLock(boolean isWriteLock, CollectionParams.LockLevel level, String collName, String shardId,
                                 String replicaName) {
    Preconditions.checkArgument(collName != null, "collName can't be null");
    Preconditions.checkArgument(level == CollectionParams.LockLevel.COLLECTION || shardId != null,
        "shardId can't be null when getting lock for shard or replica");
    Preconditions.checkArgument(level != CollectionParams.LockLevel.REPLICA || replicaName != null,
        "replicaName can't be null when getting lock for replica");

    try {
      String lockPath = makeLockPath(level, collName, shardId, replicaName);

      return isWriteLock ? new ZkDistributedLock.Write(zkClient, lockPath) : new ZkDistributedLock.Read(zkClient, lockPath);
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Creates the Zookeeper path to the lock, creating missing nodes if needed, but NOT creating the collection node.
   * If the collection node does not exist, we don't want any locking operation to succeed.
   *
   * The tree of lock directories for a given collection {@code collName} is as follows:
   * <pre>
   *   /collections
   *      collName   <-- This node has to already exist and will NOT be created
   *         Locks   <-- EPHEMERAL collection level locks go here
   *         _ShardName1
   *            Locks   <-- EPHEMERAL shard level locks go here
   *            _replicaNameS1R1   <-- EPHEMERAL replica level locks go here
   *            _replicaNameS1R2   <-- EPHEMERAL replica level locks go here
   *         _ShardName2
   *            Locks   <-- EPHEMERAL shard level locks go here
   *            _replicaNameS2R1   <-- EPHEMERAL replica level locks go here
   *            _replicaNameS2R2   <-- EPHEMERAL replica level locks go here
   * </pre>
   * Depending on the requested lock level, this method will create the path (only the parts below {@code collName}
   * where the {@code EPHEMERAL} lock nodes should go, {@code /collections/collName} will have to already exist or
   * the call will fail). That path is:
   * <ul>
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#COLLECTION} -
   *   {@code /collections/collName/Locks}</li>
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#SHARD} -
   *   {@code /collections/collName/_shardName/Locks}</li>
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#REPLICA} -
   *   {@code /collections/collName/_shardName/_replicaName}. There is no {@code Locks} subnode here because replicas do
   *   not have children so no need to separate {@code EPHEMERAL} lock nodes from children nodes as is the case for shards
   *   and collections</li>
   * </ul>
   * Note the {@code _} prefixing shards and replica names is to support shards or replicas called "{@code Locks}" (and
   * possibly, in a distant future, have other per shard files stored in Zookeeper, for example a shardState.json...).
   * Also note the returned path does not contain the separator ({@code "/"}) at the end.
   */
  private String makeLockPath(CollectionParams.LockLevel level, String collName, String shardId, String replicaName)
    throws KeeperException, InterruptedException {
    String collectionZnode = ZkStateReader.getCollectionPathRoot(collName);

    final String LOCK_NODENAME = "Locks"; // Should not start with SUBNODE_PREFIX :)
    final String SUBNODE_PREFIX = "_";

    final String[] pathElements;
    if (level == CollectionParams.LockLevel.COLLECTION) {
      pathElements = new String[]{ LOCK_NODENAME };
    } else if (level == CollectionParams.LockLevel.SHARD) {
      pathElements = new String[]{ SUBNODE_PREFIX + shardId, LOCK_NODENAME };
    } else if (level == CollectionParams.LockLevel.REPLICA) {
      pathElements = new String[]{ SUBNODE_PREFIX + shardId, SUBNODE_PREFIX + replicaName };
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported lock level " + level);
    }

    // The complete path to the lock directory
    StringBuilder sb = new StringBuilder(collectionZnode);
    for (String pathElement : pathElements) {
      sb.append(ZK_PATH_SEPARATOR);
      sb.append(pathElement);
    }
    final String lockPath = sb.toString();

    // If path already exists, bail early. This is the usual case once a given lock znode has been created once.
    // Otherwise walk the path and create missing bits. We can't call directly zkClient.makePath() because it would also
    // create the collectionZnode part of the path if it doesn't exist.
    if (!zkClient.exists(lockPath, true)) {
      // We want to create the missing elements in the path but NOT the collectionZnode part of the path
      StringBuilder walkThePath = new StringBuilder(collectionZnode);
      for (String pathElement : pathElements) {
        walkThePath.append(ZK_PATH_SEPARATOR);
        walkThePath.append(pathElement);
        try {
          // Create the next node in the path, but accept that it's already here (created previously or some other thread
          // beat us to it).
          // This call will throw a KeeperException with code() being KeeperException.Code.NONODE if the parent node does
          // not exist. In our case it's a missing /collections/collName node (or a rare/unlikely case of another thread deleting
          // the path as we create it)
          zkClient.create(walkThePath.toString(), null, CreateMode.PERSISTENT, true);
        } catch (KeeperException.NodeExistsException nee) {
          // If that element in the path already exists we're good, let's move to next.
        }
      }
    }

    return lockPath;
  }
}
