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
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by ahenrick on 4/10/14.
 */

public class TajoMasterZkController extends LeaderSelectorListenerAdapter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TajoMasterZkController.class);

  private String serverId;
  private CuratorFramework client;
  private final LeaderSelector leaderSelector;
  //private final PathChildrenCache workersCache;


  /*
   * We use one latch as barrier for the master selection
   * and another one to block the execution of master
   * operations when the ZooKeeper session transitions
   * to suspended.
   */
  private CountDownLatch leaderLatch = new CountDownLatch(1);
  private CountDownLatch closeLatch = new CountDownLatch(1);

  /**
   * Creates a new Curator client, setting the the retry policy
   * to ExponentialBackoffRetry.
   *
   * @param serverId        master identifier
   * @param hostPort    list of zookeeper servers comma-separated
   * @param retryPolicy Curator retry policy
   */
  public TajoMasterZkController(String serverId, String hostPort, RetryPolicy retryPolicy) {
    LOG.info(serverId + ": " + hostPort);

    this.serverId = serverId;
    this.client = CuratorFrameworkFactory.newClient(hostPort,
        retryPolicy);
    this.leaderSelector = new LeaderSelector(this.client, "/master", this);
  }

  public void startZK() {
    client.start();
  }

  public void bootstrap()
      throws Exception {
     if (null == client.checkExists().forPath("/workers"))  {
         client.create().forPath("/workers") ;
     }
  }

  public void runForMaster() {
    leaderSelector.setId(serverId);
    LOG.info("Starting master selection: " + serverId);
    leaderSelector.autoRequeue();
    leaderSelector.start();
  }


  public void awaitLeadership()
      throws InterruptedException {
    leaderLatch.await();
  }

  public boolean isLeader() {
    return leaderSelector.hasLeadership();
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {

    LOG.info("Mastership participants: " + serverId + ", " + leaderSelector.getParticipants());


    /*Start Tajo Master here after taking the leadership */
    LOG.info("Start Tajo Master here after taking the leadership");

    client.getCuratorListenable().addListener(masterListener);
    client.getUnhandledErrorListenable().addListener(errorsListener);

    client.setData().forPath("/master", (serverId + "=localhost:2181").getBytes());
    leaderLatch.countDown();

    /*
     * This latch is to prevent this call from exiting. If we exit, then
     * we release mastership.
     */
    closeLatch.await();
  }

  /* We use one main listener for the master. The listener processes
 * callback and watch events from various calls we make. Note that
 * many of the events related to workers and tasks are processed
 * directly by the workers cache and the tasks cache.
 */
  CuratorListener masterListener = new CuratorListener() {
    public void eventReceived(CuratorFramework client, CuratorEvent event) {
      try {
        LOG.info("Event path: " + event.getPath());
        LOG.info("Default case: " + event.getType());
      } catch (Exception e) {
        LOG.error("Exception while processing event.", e);
        try {
          close();
        } catch (IOException ioe) {
          LOG.error("IOException while closing.", ioe);
        }
      }
    }

    ;
  };


  UnhandledErrorListener errorsListener = new UnhandledErrorListener() {
    public void unhandledError(String message, Throwable e) {
      LOG.error("Unrecoverable error: " + message, e);
      try {
        close();
      } catch (IOException ioe) {
        LOG.warn("Exception when closing.", ioe);
      }
    }
  };

  @Override
  public void close()
      throws IOException {
    LOG.info("Closing");
    closeLatch.countDown();
    leaderSelector.close();
    client.close();
  }

  public static void main(String[] args) {
    try {
      TajoMasterZkController master = new TajoMasterZkController(args[0], "localhost:2181",
          new ExponentialBackoffRetry(1000, 5));
      master.startZK();
      master.bootstrap();
      master.runForMaster();
      master.awaitLeadership();

      while (master.isLeader()) {
        Thread.sleep(100);
      }
      master.close();
    } catch (Exception e) {
      LOG.error("Exception while running curator master.", e);
    }
  }

}



