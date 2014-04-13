/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;


/**
 * Created by ahenrick on 4/10/14.
 */

public class TajoWorkerZkController implements NodeCacheListener, PathChildrenCacheListener, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TajoWorkerZkController.class);

  private String workerId;
  private CuratorFramework client;
  private final NodeCache masterNode;
  private final PathChildrenCache workersCache;
  private volatile boolean connected = false;
  private volatile boolean expired = false;

  /**
   * Creates a new Curator client, setting the the retry policy
   * to ExponentialBackoffRetry.
   *
   * @param workerId    master identifier
   * @param hostPort    list of zookeeper servers comma-separated
   * @param retryPolicy Curator retry policy
   */
  public TajoWorkerZkController(String workerId, String hostPort, RetryPolicy retryPolicy) {
    LOG.info(workerId + ": " + hostPort);

    this.workerId = workerId;
    this.client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);
    this.masterNode = new NodeCache(this.client, "/master");
    this.workersCache = new PathChildrenCache(this.client, "/workers", true);
  }

  public void startZK() {
    client.start();
  }

  /**
   * Checks if this client is connected.
   *
   * @return boolean
   */
  public boolean isConnected() {
    return connected;
  }

  /**
   * Checks if ZooKeeper session is expired.
   *
   * @return
   */
  public boolean isExpired() {
    return expired;
  }


  public void bootstrap() throws Exception {
    this.masterNode.getListenable().addListener(this);
    this.workersCache.getListenable().addListener(this);
  }


  public void registerWorker() throws Exception {
    LOG.info("Starting worker selection: " + workerId);

    /*Start Tajo Worker here because it only makes sense
    to add this znode if worker is started successfully*/

    client.create().withMode(CreateMode.EPHEMERAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/workers/" + workerId, new byte[0]);
    this.masterNode.start();
    this.workersCache.start(PathChildrenCache.StartMode.NORMAL);

  }

  /**
   * Called when a change has occurred
   */
  public void nodeChanged() throws Exception {
    LOG.info("********************Master Failover Detected**************************");
    LOG.info(new String(masterNode.getCurrentData().getData()).toString());
    /*Notify Tajo Worker that the master is failed over to another node in cluste.r*/
    LOG.info("Notify Tajo Worker that the master is failed over to another node in cluster");
    LOG.info("*********************New Server is Master*************************");
  }

  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
    try {
      String workerId = event.getData().getPath().replaceFirst("/workers/", "");
      LOG.info("******************" + workerId + "****************************");

      if (this.workerId.equalsIgnoreCase(workerId)) {
        switch (event.getType()) {
          case CHILD_ADDED:
            connected = true;
            break;
          case CHILD_REMOVED:
            connected = false;
          case CONNECTION_SUSPENDED:
            connected = false;
            break;
          case CONNECTION_LOST:
            expired = true;
            connected = false;
            break;
          default:
            LOG.warn("Unknown event type : " + event);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception while trying to re-assign tasks", e);
    }
  }


  @Override
  public void close()
      throws IOException {
    LOG.info("Closing");
    masterNode.close();
    workersCache.close();
    client.close();
  }


  /**
   * Main method showing the steps to execute a worker.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {

    TajoWorkerZkController worker = new TajoWorkerZkController(args[0], "localhost:2181",
        new ExponentialBackoffRetry(1000, 5));

    worker.startZK();

    LOG.info("Connected");

      /*
         * bootstrap() create some necessary znodes.
         */
    worker.bootstrap();

        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
    worker.registerWorker();


    while (!worker.isExpired()) {
      Thread.sleep(1000);
    }

    worker.close();
  }
}

