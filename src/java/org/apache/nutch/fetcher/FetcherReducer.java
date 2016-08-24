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

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.fetcher.data.FetchItemQueues;
import org.apache.nutch.fetcher.indexer.IndexManager;
import org.apache.nutch.fetcher.server.FetcherServer;
import org.apache.nutch.mapreduce.NutchReducer;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.proxy.ProxyUpdateThread;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FetcherReducer extends NutchReducer<IntWritable, FetchEntry, String, WebPage> {

  public static final Logger LOG = FetcherJob.LOG;

  public static final int remainderLimit = 5;

  private String jobName;

  private QueueFeederThread queueFeederThread;   // feeder thread who feeds fetch queues
  private Integer fetchServerPort;
  private FetcherServer fetcherServer;
  private int fetchThreadCount = 5;
  private int maxFeedPerThread = 100;

  private FetchManager fetchManager;
  private FetchMode fetchMode = FetchMode.NATIVE;

  private long fetchJobTimeout;
  private long fetchTaskTimeout;
  private long pendingQueueCheckInterval;
  private long pendingQueueLastCheckTime;
  private long pendingTimeout;

  /** Index */
  private boolean indexJustInTime = false;
  private int indexThreadCount = 1;
  private IndexManager indexManager;

  private int minPageThroughputRate;
  private long throughputCheckTimeLimit;
  private int maxLowThroughputCount;
  private int lowThroughputCount = 0;

  /** Report interval in seconds */
  private int reportInterval;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    jobName = context.getJobName();

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);

    fetchMode = FetchMode.fromString(conf.get(Nutch.FETCH_MODE_KEY, FetchMode.NATIVE.value()));

    if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
      fetchServerPort = tryAcquireFetchServerPort();
    }

    getCounter().register(FetchManager.Counter.class);
    getReporter().silence();

    fetchJobTimeout = 60 * 1000 * NutchUtil.getUint(conf, "fetcher.timelimit.mins", Integer.MAX_VALUE / 60 / 1000 / 2);
    fetchTaskTimeout = 60 * 1000 * NutchUtil.getUint(conf, "mapred.task.timeout.mins", Integer.MAX_VALUE / 60 / 1000 / 2);
    pendingQueueCheckInterval = 60 * 1000 * conf.getLong("fetcher.pending.queue.check.time.mins", 8);
    pendingTimeout = 60 * 1000 * conf.getLong("fetcher.pending.timeout.mins", pendingQueueCheckInterval * 2);
    pendingQueueLastCheckTime = startTime;
    fetchThreadCount = conf.getInt("fetcher.threads.fetch", 5);
    maxFeedPerThread = conf.getInt("fetcher.queue.depth.multiplier", 100);
    reportInterval = conf.getInt("fetcher.pending.timeout.secs", 20);

    /**
     * Used for threshold check, holds pages and bytes processed in the last sec
     * We should keep a minimal fetch speed
     * */
    minPageThroughputRate = conf.getInt("fetcher.throughput.threshold.pages", -1);
    maxLowThroughputCount = conf.getInt("fetcher.throughput.threshold.sequence", 10);
    throughputCheckTimeLimit = startTime + conf.getLong("fetcher.throughput.threshold.check.after", 30 * 1000);

    indexJustInTime = conf.getBoolean(Nutch.INDEX_JUST_IN_TIME, false);
    if (indexJustInTime) {
      indexManager = new IndexManager(conf);
    }
    fetchManager = FetchManagerPool.getInstance().create(context.getJobID().getId(), indexManager, getCounter(), context);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "fetchMode", fetchMode,
        "fetchManagerPool", FetchManagerPool.getInstance().name(),
        "fetchManager", fetchManager.name(),
        "fetchJobTimeout(m)", fetchJobTimeout / 60 / 1000,
        "fetchTaskTimeout(m)", fetchTaskTimeout / 60 / 1000,
        "pendingQueueCheckInterval(m)", pendingQueueCheckInterval / 60 / 1000,
        "pendingTimeout(m)", pendingTimeout / 60 / 1000,
        "pendingQueueLastCheckTime", TimingUtil.format(pendingQueueLastCheckTime),
        "fetchThreadCount", fetchThreadCount,
        "maxFeedPerThread", maxFeedPerThread,
        "fetchServerPort", fetchServerPort,
        "minPageThroughputRate", minPageThroughputRate,
        "maxLowThroughputCount", maxLowThroughputCount,
        "throughputCheckTimeLimit", TimingUtil.format(throughputCheckTimeLimit),
        "reportInterval(s)", reportInterval,
        "indexJustInTime", indexJustInTime,
        "indexThreadCount", indexThreadCount
    ));

    LOG.info("use the following command to finishi the task : \n>>>\n"
        + "echo finish " + jobName + " >> /tmp/.NUTCH_LOCAL_FILE_COMMAND" + "\n<<<");
  }

  @Override
  protected void doRun(Context context) throws IOException, InterruptedException {
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

    if (indexJustInTime) {
      startIndexThreads(context);
    }

    startCheckAndReportLoop(context);
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

  private Integer tryAcquireFetchServerPort() {
    int port = FetcherServer.acquirePort(conf);
    if (port < 0) {
      LOG.error("Failed to acquire fetch server port");
      stop();
    }
    return port;
  }

  private void startFetchServer(final Configuration conf, final int port) {
    fetcherServer = FetcherServer.startInDaemonThread(conf, port);
  }

  /**
   * Start crowd sourcing threads
   * Blocking
   * */
  private void startCrowdsourcingThreads(Context context) {
    fetchManager.startFetchThreads(queueFeederThread, fetchThreadCount, context);
  }

  /**
   * Start native fetcher threads
   * Blocking
   * */
  private void startNativeFetcherThreads(Context context) {
    fetchManager.startFetchThreads(queueFeederThread, fetchThreadCount, context);
  }

  /**
   * Start index threads
   * Blocking
   * */
  private void startIndexThreads(Context context) throws IOException {
    indexManager.startIndexThreads(indexThreadCount, context);
  }

  private void startCheckAndReportLoop(Context context) throws IOException {
    do {
      fetchManager.adjustFetchResourceByTargetBandwidth(queueFeederThread);

      fetchManager.waitAndReport(reportInterval, context);

      long now = System.currentTimeMillis();
      long jobTime = now - startTime;
      long idleTime = now - fetchManager.getLastTaskFinishTime();
      boolean isCrowdsourcing = fetchMode.equals(FetchMode.CROWDSOURCING);

      /**
       * In crowd sourcing mode, check if any fetch tasks are hung
       * */
      if (isCrowdsourcing) {
        checkPendingQueue(now, idleTime);
      }

      /**
       * Dump the remainder fetch items if feeder thread is no available and fetch item is few
       * */
      if (fetchManager.getQueueCount() <= remainderLimit) {
        handleFewFetchItems();
      }

      /**
       * All fetch tasks are finished
       * */
      if (isMissionComplete()) {
        LOG.info("All done, exit the job...");
        break;
      }

      if (jobTime > fetchJobTimeout) {
        handleJobTimeout();
        LOG.info("Hit fetch job timeout " + jobTime / 1000 + "s, exit the job...");
        break;
      }

      /**
       * No new tasks for too long, some requests seem to hang. We exits the job.
       * */
      if (idleTime > fetchTaskTimeout) {
        handleFetchTaskTimeout();
        LOG.info("Hit fetch task timeout " + idleTime / 1000 + "s, exit the job...");
        break;
      }

      /**
       * Check throughput(fetch speed)
       * */
      float fetchSpeed = fetchManager.getPagesThroughputRate();
      if (now > throughputCheckTimeLimit && fetchSpeed < minPageThroughputRate) {
        checkFetchEfficiency(context);
        if (lowThroughputCount > maxLowThroughputCount) {
          break;
        }
      }

      /**
       * Read local filesystem for control commands
       * */
      if (checkLocalFileCommandExists("finish " + jobName)) {
        handleFinishJobCommand();
        LOG.info("Find finish-job command in local command file " + Nutch.NUTCH_LOCAL_COMMAND_FILE + ", exit the job...");
        break;
      }
    } while (fetchManager.getActiveFetchThreadCount() > 0);


  }

  /**
   * TODO : check the real meaning of timeLimitMillis
   *
   * It seems to be "fetch deadline"
   * */
  public int handleJobTimeout() {
    return fetchManager.clearFetchItemQueues();
  }

  /**
   * Check if some threads are hung. If so, we should stop the main fetch loop
   * should we stop the main fetch loop
   * */
  private void handleFetchTaskTimeout() {
    int activeFetchThreads = fetchManager.getActiveFetchThreadCount();
    if (activeFetchThreads <= 0) {
      return;
    }

    LOG.warn("Aborting with " + activeFetchThreads + " hung threads.");

    fetchManager.dumpFetchThreads();
  }

  private void handleFewFetchItems() {
    if (!isFeederAlive()) {
      fetchManager.dumpFetchItems(remainderLimit);
    }
  }

  private void handleFinishJobCommand() {
    queueFeederThread.complete();
    indexManager.halt();

    fetchManager.clearFetchItemQueues();
    fetchManager.dumpFetchItems(remainderLimit);
  }

  /**
   * Check local command file.
   * */
  private boolean checkLocalFileCommandExists(String command) {
    boolean exist = false;

    Path path = Paths.get(Nutch.NUTCH_LOCAL_COMMAND_FILE);
    if (Files.exists(path)) {
      try {
        List<String> lines = Files.readAllLines(path);
        exist = lines.stream().anyMatch(line -> line.equals(command));
        lines.remove(command);
        Files.write(path, lines);
      } catch (IOException e) {
      }
    }

    return exist;
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

  /**
   * Check pending queue to see if some item is expired,
   * which often means the fetch client running into wrong
   * */
  private void checkPendingQueue(long now, long idleTime) {
    if (fetchManager.getReadyItemCount() + fetchManager.getPendingItemCount() < 10) {
      pendingTimeout = 2 * 60 * 1000;
    }

    boolean shouldCheck = now > pendingQueueLastCheckTime + 2 * reportInterval * 1000;
    if (shouldCheck) {
      shouldCheck = idleTime > pendingTimeout || now - pendingQueueLastCheckTime > pendingQueueCheckInterval;
    }

    if (shouldCheck) {
      LOG.info("Checking pending items ...");
      fetchManager.reviewPendingFetchItems(false);
      pendingQueueLastCheckTime = now;
    }
  }

  /**
   * Check if we're dropping below the threshold (we are too slow)
   * */
  private int checkFetchEfficiency(Context context) throws IOException {
    lowThroughputCount++;

    LOG.warn(lowThroughputCount + ": Fetch speed too slow, minimum speed should be " + minPageThroughputRate + " pages per second");

    // Quit if we dropped below threshold too many times
    if (lowThroughputCount > maxLowThroughputCount) {
      LOG.warn("Dropped below threshold too many times in a row, killing!");

      // Disable the threshold checker
      minPageThroughputRate = -1;

      // Empty the queues cleanly and get number of items that were dropped
      // TODO : make this queue specified, because some fetch queue might be faster
      int hitByThrougputThreshold = fetchManager.clearFetchItemQueues();

      if (hitByThrougputThreshold != 0) {
        context.getCounter("Runtime Status", "unacceptableFetchEfficiency").increment(hitByThrougputThreshold);
      }
    }

    return lowThroughputCount;
  }
}
