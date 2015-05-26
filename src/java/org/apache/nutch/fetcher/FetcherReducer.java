/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.fetcher;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.api.NutchServer;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.fetcher.data.FetchItemQueues;
import org.apache.nutch.fetcher.server.FetcherServer;
import org.apache.nutch.mapreduce.NutchReducer;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.net.proxy.ProxyUpdateThread;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NetUtil;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

public class FetcherReducer extends NutchReducer<IntWritable, FetchEntry, String, WebPage> {

  public static final Logger LOG = FetcherJob.LOG;

  private QueueFeederThread queueFeederThread;   // feeder thread who feeds fetch queues
  private Integer fetchServerPort;
  private FetcherServer fetcherServer;
  private final List<FetchThread> fetchThreads = Lists.newArrayList();
  private int fetchThreadCount = 5;
  private int maxFeedPerThread = 100;

  private FetchManager fetchManager;
  private FetchMode fetchMode = FetchMode.NATIVE;

  private long fetchJobTimeout;
  private long pendingQueueCheckInterval;
  private long pendingQueueLastCheckTime;
  private long pendingTimeout;
  private int reportIntervalSec;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    fetchMode = FetchMode.fromString(conf.get("fetcher.fetch.mode", "native"));

    fetchServerPort = FetcherServer.acquirePort(conf);
    fetchManager = new FetchManager(context.getJobID().getId(), getCounter(), context);
    FetchManagerPool.getInstance().put(fetchManager);

    getCounter().register(FetchManager.Counter.class);
    getReporter().silence();

    fetchJobTimeout = 60 * 1000 * conf.getInt("mapred.task.timeout.mins", 10);
    pendingQueueCheckInterval = 60 * 1000 * conf.getLong("fetcher.pending.queue.check.time.mins", 8);
    pendingTimeout = 60 * 1000 * conf.getLong("fetcher.pending.timeout.mins", 3);
    reportIntervalSec = conf.getInt("fetcher.pending.timeout.secs", 20);
    pendingQueueLastCheckTime = startTime;
    fetchThreadCount = conf.getInt("fetcher.threads.fetch", 5);
    maxFeedPerThread = conf.getInt("fetcher.queue.depth.multiplier", 100);

    LOG.info(NutchUtil.printArgMap(
        "fetchMode", fetchMode,
        "fetchJobTimeout", fetchJobTimeout,
        "pendingQueueCheckInterval", pendingQueueCheckInterval,
        "pendingTimeout", pendingTimeout,
        "reportIntervalSec", reportIntervalSec,
        "pendingQueueLastCheckTime", pendingQueueLastCheckTime,
        "fetchThreadCount", fetchThreadCount,
        "maxFeedPerThread", maxFeedPerThread,
        "fetchServerPort", fetchServerPort
    ));
  }

  @Override
  protected void doRun(Context context) throws IOException, InterruptedException {
    if (fetchServerPort == null) {
      LOG.error("Failed to acquire fetch server port");
      stop();
    }

    // Queue feeder thread
    startQueueFeederThread(context);

    if (FetchMode.CROWDSOURCING.equals(fetchMode)) {
      startCrowdsourcingThreads(context);

      startFetchServer(conf, fetchServerPort);
    }
    else {
      if (FetchMode.PROXY.equals(fetchMode)) {
        ProxyUpdateThread proxyUpdateThread = new ProxyUpdateThread(conf);
        proxyUpdateThread.start();
      }

      // Threads for native or proxy mode
      startNativeFetcherThreads(context);
    }

    checkAndReportFetcherStatus(context);
  }

  @Override
  protected void cleanup(Context context) {
    FetchManagerPool.getInstance().remove(context.getJobID().getId());
    if (fetcherServer != null && fetcherServer.isRunning()) {
      fetcherServer.stop(true);
    }

    super.cleanup(context);
  }

  /**
   * Start queue feeder thread. The thread fetches webpages from the reduce result
   * and add it into the fetch queue
   * Non-Blocking
   * @throws InterruptedException 
   * @throws IOException 
   * */
  public void startQueueFeederThread(Context context) throws IOException, InterruptedException {
    FetchItemQueues queues = fetchManager.getFetchItemQueues();
    queueFeederThread = new QueueFeederThread(context, queues, fetchThreadCount * maxFeedPerThread);
    queueFeederThread.start();
  }

  private boolean isFeederAlive() {
    Validate.notNull(queueFeederThread);

    return queueFeederThread.isAlive();
  }

  private boolean isMissionComplete() {
    return !isFeederAlive() 
        && fetchManager.getReadyItemCount() == 0 
        && fetchManager.getPendingItemCount() == 0;
  }

  private void startFetchServer(final Configuration conf, final int port) {
    fetcherServer = FetcherServer.startInDaemonThread(conf, port);
  }

  /**
   * Blocking
   * */
  private void startCrowdsourcingThreads(Context context) {
    // Start native fetch threads to handle fetch result from fetch clients
    for (int i = 0; i < fetchThreadCount; i++) {
      FetchThread fetchThread = new FetchThread(queueFeederThread, fetchManager, context);
      fetchThreads.add(fetchThread);
      fetchThread.start();
    }
  }

  private void checkAndReportCrowdsourcingFetcherStatus(Context context) throws IOException {
    boolean shouldStop = false;
    do {
      fetchManager.waitAndReport(context, reportIntervalSec, isFeederAlive());

      long now = System.currentTimeMillis();
      long idleTime = now - fetchManager.getLastTaskFinishTime();

      checkPendingQueue(now, idleTime);

      if (!shouldStop && idleTime > fetchJobTimeout) {
        LOG.info("Hit fetch job timeout " + idleTime / 1000 + "s, exit the job...");
        shouldStop = true;
      }

      // All fetch tasks are finished
      if (!shouldStop && isMissionComplete()) {
        LOG.info("All done, exit the job...");
        shouldStop = true;
      }

    } while (!shouldStop);
  }

  private void startNativeFetcherThreads(Context context) {
    for (int i = 0; i < fetchThreadCount; i++) {
      FetchThread fetchThread = new FetchThread(queueFeederThread, fetchManager, context);
      fetchThreads.add(fetchThread);
      fetchThread.start();
    }
  }

  // Blocking
  private void checkAndReportFetcherStatus(Context context) throws IOException {
    if (FetchMode.CROWDSOURCING.equals(fetchMode)) {
      checkAndReportCrowdsourcingFetcherStatus(context);
    }
    else {
      checkAndReportNativeFetcherStatus(context);      
    }
  }

  private void checkAndReportNativeFetcherStatus(Context context) throws IOException {
    // Used for threshold check, holds pages and bytes processed in the last sec
    int throughputThresholdCurrentSequence = 0;

    int throughputThresholdPages = conf.getInt("fetcher.throughput.threshold.pages", -1);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: throughput threshold: " + throughputThresholdPages); }
    int throughputThresholdSequence = conf.getInt("fetcher.throughput.threshold.sequence", 5);
    if (LOG.isInfoEnabled()) {
      LOG.info("Fetcher: throughput threshold sequence: " + throughputThresholdSequence);
    }
    long throughputThresholdTimeLimit = conf.getLong("fetcher.throughput.threshold.check.after", -1);

    do {
      float pagesLastSec = fetchManager.waitAndReport(context, reportIntervalSec, isFeederAlive());

      // if throughput threshold is enabled
      if (throughputThresholdTimeLimit < System.currentTimeMillis() && throughputThresholdPages != -1) {
        // Check if we're dropping below the threshold
        if (pagesLastSec < throughputThresholdPages) {
          throughputThresholdCurrentSequence++;

          LOG.warn(Integer.toString(throughputThresholdCurrentSequence) 
              + ": dropping below configured threshold of " + Integer.toString(throughputThresholdPages) 
              + " pages per second");

          // Quit if we dropped below threshold too many times
          if (throughputThresholdCurrentSequence > throughputThresholdSequence) {
            LOG.warn("Dropped below threshold too many times in a row, killing!");

            // Disable the threshold checker
            throughputThresholdPages = -1;

            // Empty the queues cleanly and get number of items that were dropped
            int hitByThrougputThreshold = fetchManager.clearFetchItemQueues();

            if (hitByThrougputThreshold != 0) {
              context.getCounter("FetcherStatus", "hitByThrougputThreshold").increment(hitByThrougputThreshold);
            }
          }
        } else {
          throughputThresholdCurrentSequence = 0;
        }
      }

      // some requests seem to hang, despite all intentions
      if ((System.currentTimeMillis() - fetchManager.getLastTaskStartTime()) > fetchJobTimeout) {
        if (fetchManager.activeFetcherThreads.get() > 0) {
          LOG.warn("Aborting with " + fetchManager.activeFetcherThreads.get() + " hung threads.");

          for (int i = 0; i < fetchThreads.size(); i++) {
            FetchThread thread = fetchThreads.get(i);
            if (thread.isAlive()) {
              LOG.warn("Thread #" + i + " hung while processing " + thread.reprUrl());

              if (LOG.isDebugEnabled()) {
                StackTraceElement[] stack = thread.getStackTrace();
                StringBuilder sb = new StringBuilder();
                sb.append("Stack of thread #").append(i).append(":\n");
                for (StackTraceElement s : stack) {
                  sb.append(s.toString()).append('\n');
                }

                LOG.debug(sb.toString());
              }
            }
          } // for
        } // if

        return;
      } // if
    } while (fetchManager.activeFetcherThreads.get() > 0);
  }

  /**
   * Check pending queue to see if some item is expired,
   * which often means the fetch client running into wrong
   * TODO : may move this method into FetchManager
   * */
  private void checkPendingQueue(long now, long idleTime) {
    if (fetchManager.getReadyItemCount() + fetchManager.getPendingItemCount() < 10) {
      pendingTimeout = 2 * 60 * 1000;
    }

    boolean shouldCheck = now > pendingQueueLastCheckTime + 2 * reportIntervalSec * 1000;
    if (shouldCheck) {
      shouldCheck = idleTime > pendingTimeout || now - pendingQueueLastCheckTime > pendingQueueCheckInterval;
    }

    if (shouldCheck) {
      LOG.info("Check pending items");
      fetchManager.reviewPendingFetchItems(false);
      pendingQueueLastCheckTime = now;
    }
  }
}
