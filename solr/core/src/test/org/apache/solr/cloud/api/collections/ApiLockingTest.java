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

import java.nio.file.Path;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.DistributedLock;
import org.apache.solr.cloud.ZkDistributedLockFactory;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class ApiLockingTest  extends SolrTestCaseJ4 {
  private static final String COLLECTION_NAME = "lockColl";
  final String SHARD1_NAME = "lockShard1";
  final String SHARD2_NAME = "lockShard2";
  final String REPLICA_NAME = "lockReplica";

  static final int TIMEOUT = 10000;


  /**
   * Tests the multi locking needed for Collection API command execution
   */
  @Test
  public void testCollectionApiLockHierarchy() throws Exception {
    Path zkDir = createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT)) {
        ApiLockingHelper apiLockingHelper = new ApiLockingHelper(new ZkDistributedLockFactory(zkClient));

        try {
          apiLockingHelper.getCollectionApiLocks(CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
          fail("Collection does not exist, lock creation should have failed");
        } catch (SolrException expected) {
        }

        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + COLLECTION_NAME, null, CreateMode.PERSISTENT, true);

        // Lock at collection level (which prevents locking + acquiring on any other level of the hierarchy)
        List<DistributedLock> collLocks = apiLockingHelper.getCollectionApiLocks(CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
        assertTrue("Collection should have been acquired", isAcquired(collLocks));
        assertEquals("Lock at collection level expected to need one distributed lock", 1, collLocks.size());

        // Request a shard lock. Will not be acquired as long as we don't release the collection lock above
        List<DistributedLock> shard1Locks = apiLockingHelper.getCollectionApiLocks(CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD1_NAME, null);
        assertFalse("Shard1 should not have been acquired", isAcquired(shard1Locks));
        assertEquals("Lock at shard level expected to need two distributed locks", 2, shard1Locks.size());

        // Request a lock on another shard. Will not be acquired as long as we don't release the collection lock above
        List<DistributedLock> shard2Locks = apiLockingHelper.getCollectionApiLocks(CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD2_NAME, null);
        assertFalse("Shard2 should not have been acquired", isAcquired(shard2Locks));

        assertTrue("Collection should still be acquired", isAcquired(collLocks));

        apiLockingHelper.releaseLocks(collLocks);

        assertTrue("Shard1 should have been acquired now that collection lock released", isAcquired(shard1Locks));
        assertTrue("Shard2 should have been acquired now that collection lock released", isAcquired(shard2Locks));

        // Request a lock on replica of shard1
        List<DistributedLock> replicaShard1Locks = apiLockingHelper.getCollectionApiLocks(CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD1_NAME, REPLICA_NAME);
        assertFalse("replicaShard1Locks should not have been acquired, shard1 is locked", isAcquired(replicaShard1Locks));

        // Now ask for a new lock on the collection
        collLocks = apiLockingHelper.getCollectionApiLocks(CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);

        assertFalse("Collection should not have been acquired, shard1 and shard2 locks preventing it", isAcquired(collLocks));

        apiLockingHelper.releaseLocks(shard1Locks);
        assertTrue("replicaShard1Locks should have been acquired, as shard1 got released", isAcquired(replicaShard1Locks));
        assertFalse("Collection should not have been acquired, shard2 lock is preventing it", isAcquired(collLocks));

        apiLockingHelper.releaseLocks(replicaShard1Locks);

        // Request a lock on replica of shard2
        List<DistributedLock> replicaShard2Locks = apiLockingHelper.getCollectionApiLocks(CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD2_NAME, REPLICA_NAME);
        assertFalse("replicaShard2Locks should not have been acquired, shard2 is locked", isAcquired(replicaShard2Locks));

        apiLockingHelper.releaseLocks(shard2Locks);

        assertTrue("Collection should have been acquired as shard2 got released and replicaShard2Locks was requested after the collection lock", isAcquired(collLocks));
        assertFalse("replicaShard2Locks should not have been acquired, collLocks is locked", isAcquired(replicaShard2Locks));

        apiLockingHelper.releaseLocks(collLocks);
        assertTrue("replicaShard2Locks should have been acquired, the collection lock got released", isAcquired(replicaShard2Locks));
      }
    } finally {
      server.shutdown();
    }
  }

  private boolean isAcquired(List<DistributedLock> locks) {
    for (DistributedLock lock : locks) {
      if (!lock.isAcquired()) {
        return false;
      }
    }
    return true;
  }


}
