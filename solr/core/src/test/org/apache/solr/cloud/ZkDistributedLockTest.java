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

import java.nio.file.Path;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class ZkDistributedLockTest extends SolrTestCaseJ4 {

  private static final String COLLECTION_NAME = "lockColl";
  final String SHARD_NAME = "lockShard";
  final String REPLICA_NAME = "lockReplica";

  static final int TIMEOUT = 10000;

  /**
   * Tests the obtention of a single read or write lock at a specific hierarchical level
   */
  @Test
  public void testSingleLocks() throws Exception {
    Path zkDir = createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT)) {
        DistributedLockFactory factory = new ZkDistributedLockFactory(zkClient);

        try {
          factory.createLock(true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
          fail("Collection does not exist, lock creation should have failed");
        } catch (SolrException expected) {
        }

        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + COLLECTION_NAME, null, CreateMode.PERSISTENT, true);

        // Collection level locks
        DistributedLock collRL1 = factory.createLock(false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
        assertTrue("collRL1 should have been acquired", collRL1.isAcquired());

        DistributedLock collRL2 = factory.createLock(false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
        assertTrue("collRL1 should have been acquired", collRL2.isAcquired());

        DistributedLock collWL3 = factory.createLock(true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
        assertFalse("collWL3 should not have been acquired, due to collRL1 and collRL2", collWL3.isAcquired());

        assertTrue("collRL2 should have been acquired, that should not have changed", collRL2.isAcquired());

        collRL1.release();
        collRL2.release();
        assertTrue("collWL3 should have been acquired, collRL1 and collRL2 were released", collWL3.isAcquired());

        DistributedLock collRL4 = factory.createLock(false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
        assertFalse("collRL4 should not have been acquired, due to collWL3 locking the collection", collRL4.isAcquired());

        // Collection is write locked by collWL3 and collRL4 read lock waiting behind. Now moving to request shard level locks.
        // These are totally independent from the Collection level locks so should see no impact.
        DistributedLock shardWL5 = factory.createLock(true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null);
        assertTrue("shardWL5 should have been acquired, there is no lock on that shard", shardWL5.isAcquired());

        DistributedLock shardWL6 = factory.createLock(true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null);
        assertFalse("shardWL6 should not have been acquired, shardWL5 is locking that shard", shardWL6.isAcquired());

        // Get a lock on a Replica. Again this is independent of collection or shard level
        DistributedLock replicaRL7 = factory.createLock(false, CollectionParams.LockLevel.REPLICA, COLLECTION_NAME, SHARD_NAME, REPLICA_NAME);
        assertTrue("replicaRL7 should have been acquired", replicaRL7.isAcquired());

        DistributedLock replicaWL8 = factory.createLock(true, CollectionParams.LockLevel.REPLICA, COLLECTION_NAME, SHARD_NAME, REPLICA_NAME);
        assertFalse("replicaWL8 should not have been acquired, replicaRL7 is read locking that replica", replicaWL8.isAcquired());

        replicaRL7.release();
        assertTrue("replicaWL8 should have been acquired, as replicaRL7 got released", replicaWL8.isAcquired());


        collWL3.release();
        assertTrue("collRL4 should have been acquired given collWL3 released", collRL4.isAcquired());
        shardWL5.release();
        assertTrue("shardWL6 should have been acquired, now that shardWL5 was released", shardWL6.isAcquired());

        replicaWL8.release();
        try {
          replicaWL8.isAcquired();
          fail("isAcquired() called after release() on a lock should have thrown exception");
        } catch (IllegalStateException ise) {
          // expected
        }
      }
    } finally {
      server.shutdown();
    }
  }

}
