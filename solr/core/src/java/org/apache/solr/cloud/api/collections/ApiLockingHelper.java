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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.cloud.DistributedLock;
import org.apache.solr.cloud.DistributedLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;

/**
 * This class implements higher level locking abstractions for the Collection API using lower level read and write locks.
 */
public class ApiLockingHelper {
  private final DistributedLockFactory lockFactory;

  ApiLockingHelper(DistributedLockFactory lockFactory) {
    this.lockFactory = lockFactory;
  }

  /**
   * For the {@link org.apache.solr.common.params.CollectionParams.LockLevel} of the passed {@code action}, obtains the
   * required locks (if any) and returns.<p>
   *
   * This method obtains a write lock at the actual level and path of the action, and also obtains read locks on "lower"
   * lock levels. For example for a lock at the shard level, a write lock will be requested at the corresponding shard path
   * and a read lock on the corresponding collection path (in order to prevent an operation locking at the collection level
   * from executing concurrently with an operation on one of the shards of the collection).
   * See documentation linked to SOLR-14840 regarding Collection API locking.
   *
   * @return the required locks that once all are {@link DistributedLock#isAcquired()} guarantee the corresponding Collection
   * API command can execute safely. If they're going to be {@link DistributedLock#waitUntilAcquired()} (which will be the case
   * except in some tests) the calls <b>must be done</b> in the returned list order, otherwise deadlock risk ({@link #waitUntilAcquiredLocks}
   * will handle that).
   * Returned locks <b>MUST</b> be {@link DistributedLock#release()} no matter what once no longer needed as they prevent
   * other threads from locking  ({@link #releaseLocks} will handle that).
   */
  List<DistributedLock> getCollectionApiLocks(CollectionParams.LockLevel lockLevel, String collName, String shardId, String replicaName) {
    if (lockLevel == CollectionParams.LockLevel.NONE) {
      return List.of();
    }

    if (lockLevel == CollectionParams.LockLevel.CLUSTER) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Bug. Not expecting locking at cluster level.");
    }

    if (collName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Bug. collName can't be null");
    }

    // The first requested lock is a write one (on the target object for the action, depending on lock level), then requesting
    // read locks on "higher" levels (collection > shard > replica here for the level. Note LockLevel "height" is other way around).
    boolean requestWriteLock = true;
    final CollectionParams.LockLevel iterationOrder[] = { CollectionParams.LockLevel.REPLICA, CollectionParams.LockLevel.SHARD, CollectionParams.LockLevel.COLLECTION };
    List<DistributedLock> locks = new ArrayList<>(iterationOrder.length);
    for (CollectionParams.LockLevel level : iterationOrder) {
      // This comparison is based on the LockLevel height value that classifies replica > shard > collection.
      if (lockLevel.isHigherOrEqual(level)) {
        locks.add(lockFactory.createLock(requestWriteLock, level, collName, shardId, replicaName));
        requestWriteLock = false;
      }
    }

    return locks;
  }

  void waitUntilAcquiredLocks(List<DistributedLock> locks) {
    for (DistributedLock lock : locks) {
      lock.waitUntilAcquired();
    }
  }


  void releaseLocks(List<DistributedLock> locks) {
    for (DistributedLock lock : locks) {
      lock.release();
    }
  }
}
