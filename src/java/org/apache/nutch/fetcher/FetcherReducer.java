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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.fetcher.data.FetchItemQueues;
import org.apache.nutch.fetcher.indexer.IndexManager;
import org.apache.nutch.fetcher.indexer.IndexThread;
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
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;

public class FetcherReducer extends NutchReducer<IntWritable, FetchEntry, String, WebPage> {

  public static final Logger LOG = FetcherJob.LOG;

  public static final int QUEUE_REMAINDER_LIMIT = 5;

  private String jobName;
  private FetchMode fetchMode = FetchMode.NATIVE;

  /** Threads */
  private QueueFeederThread queueFeederThread;
  private int maxFeedPerThread = 100;
  private Integer fetchServerPort;
  private FetcherServer fetcherServer;
  private int fetchThreadCount = 5;

  private FetchManagerPool fetchManagerPool;
  private FetchManager fetchManager;

  /** Time out control */
  private long fetchJobTimeout;
  private long fetchTaskTimeout;
  private long pendingQueueCheckInterval;
  private long pendingQueueLastCheckTime;
  private long pendingTimeout;

  /** Throughput control */
  private int minPageThroughputRate;
  private long throughputCheckTimeLimit;
  private int maxLowThroughputCount;
  private int maxTotalLowThroughputCount;
  private int lowThroughputCount = 0;
  private int totalLowThroughputCount = 0;

  /** Index */
  private boolean indexJustInTime = false;
  private int indexThreadCount = 1;
  private IndexManager indexManager;

  /** Report */
  private int reportIntervalSec;

  private String nutchTmpDir;
  private String finishScript;
  private String commandFile;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(FetchManager.Counter.class);
    getReporter().silence();

    jobName = context.getJobName();

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);

    fetchMode = FetchMode.fromString(conf.get(Nutch.FETCH_MODE_KEY, FetchMode.NATIVE.value()));
    if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
      fetchServerPort = tryAcquireFetchServerPort();
    }
    fetchThreadCount = conf.getInt("fetcher.threads.fetch", 5);
    maxFeedPerThread = conf.getInt("fetcher.queue.depth.multiplier", 100);

    fetchJobTimeout = 60 * 1000 * NutchUtil.getUint(conf, "fetcher.timelimit.mins", Integer.MAX_VALUE / 60 / 1000 / 2);
    fetchTaskTimeout = 60 * 1000 * NutchUtil.getUint(conf, "mapred.task.timeout.mins", Integer.MAX_VALUE / 60 / 1000 / 2);
    pendingQueueCheckInterval = 60 * 1000 * conf.getLong("fetcher.pending.queue.check.time.mins", 8);
    pendingTimeout = 60 * 1000 * conf.getLong("fetcher.pending.timeout.mins", pendingQueueCheckInterval * 2);
    pendingQueueLastCheckTime = startTime;

    /**
     * Used for threshold check, holds pages and bytes processed in the last sec
     * We should keep a minimal fetch speed
     * */
    minPageThroughputRate = conf.getInt("fetcher.throughput.threshold.pages", -1);
    maxLowThroughputCount = conf.getInt("fetcher.throughput.threshold.sequence", 10);
    maxTotalLowThroughputCount = maxLowThroughputCount * 10;
    throughputCheckTimeLimit = startTime + conf.getLong("fetcher.throughput.threshold.check.after", 30 * 1000);

    // index manager
    indexJustInTime = conf.getBoolean(Nutch.INDEX_JUST_IN_TIME, false);
    indexManager = indexJustInTime ? new IndexManager(conf) : null;

    // fetch manager
    fetchManagerPool = FetchManagerPool.getInstance();
    fetchManager = fetchManagerPool.create(indexManager, getCounter(), context);

    // report
    reportIntervalSec = conf.getInt("fetcher.pending.timeout.secs", 20);

    // scripts
    nutchTmpDir = conf.get("nutch.tmp.dir", Nutch.NUTCH_TMP_DIR);
    commandFile = Nutch.NUTCH_LOCAL_COMMAND_FILE;
    finishScript = nutchTmpDir + "/scripts/finish_" + jobName + ".sh";

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "fetchMode", fetchMode,

        "fetchThreadCount", fetchThreadCount,
        "maxFeedPerThread", maxFeedPerThread,
        "fetchServerPort", fetchServerPort,

        "fetchManagerPool", FetchManagerPool.getInstance().name(),
        "fetchManager", fetchManager.name(),

        "fetchJobTimeout(m)", fetchJobTimeout / 60 / 1000,
        "fetchTaskTimeout(m)", fetchTaskTimeout / 60 / 1000,
        "pendingQueueCheckInterval(m)", pendingQueueCheckInterval / 60 / 1000,
        "pendingTimeout(m)", pendingTimeout / 60 / 1000,
        "pendingQueueLastCheckTime", TimingUtil.format(pendingQueueLastCheckTime),

        "minPageThroughputRate", minPageThroughputRate,
        "maxLowThroughputCount", maxLowThroughputCount,
        "throughputCheckTimeLimit", TimingUtil.format(throughputCheckTimeLimit),

        "reportIntervalSec(s)", reportIntervalSec,
        "indexJustInTime", indexJustInTime,
        "indexThreadCount", indexThreadCount
    ));

    generateFinishCommand();
  }

  private void generateFinishCommand() throws IOException {
    String cmd = "#bin\necho finish " + jobName + " >> " + commandFile;

    Path script = Paths.get(finishScript);
    Files.createDirectories(script.getParent());
    Files.write(script, cmd.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwxrw-r--"));

    LOG.info("Script to finish this job : " + script.toAbsolutePath());
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
    try {
      // LOG.info("Clean up reducer, job name : " + context.getJobName());

      if (fetcherServer != null && fetcherServer.isRunning()) {
        fetcherServer.stop(true);
      }

      if (indexManager != null && !indexManager.isHalted()) {
        indexManager.halt();
      }

      if (queueFeederThread != null && !queueFeederThread.isCompleted()) {
        queueFeederThread.complete();
      }

      if (fetchManager != null) {
        LOG.info(">>>>>> [Final Status] >>>>>>");
        LOG.info("[Final]" + context.getStatus());

        fetchManager.dumpFetchItems(QUEUE_REMAINDER_LIMIT);
        fetchManager.reportUnreachableHosts();
        fetchManager.reportSlowHosts();

        fetchManager.clearReadyTasks();
        LOG.info("<<<<<< [End Final Status] <<<<<<");
      }

      Files.delete(Paths.get(finishScript));
    }
    catch (Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
    finally {
      fetchManagerPool.remove(fetchManager.getId());

      super.cleanup(context);
    }
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
  public void startIndexThreads(Context context) throws IOException {
    if (indexManager == null) {
      return;
    }

    for (int i = 0; i < indexThreadCount; i++) {
      IndexThread indexThread = new IndexThread(indexManager, context);
      indexThread.start();
    }
  }

  private void startCheckAndReportLoop(Context context) throws IOException {
    do {
      fetchManager.adjustFetchResourceByTargetBandwidth(queueFeederThread);

      fetchManager.waitAndReport(reportIntervalSec, context);

      long now = System.currentTimeMillis();
      long jobTime = now - startTime;
      long idleTime = now - fetchManager.getLastTaskFinishTime();

      /**
       * In crowd sourcing mode, check if any fetch tasks are hung
       * */
      checkPendingQueue(now, idleTime);

      /**
       * Dump the remainder fetch items if feeder thread is no available and fetch item is few
       * */
      if (fetchManager.getQueueCount() <= QUEUE_REMAINDER_LIMIT) {
        handleFewFetchItems();
      }

      /**
       * Check throughput(fetch speed)
       * */
      float fetchSpeed = fetchManager.getPagesThroughputRate();
      if (now > throughputCheckTimeLimit && fetchSpeed < minPageThroughputRate) {
        checkFetchEfficiency(context);
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
       * Read local filesystem for control commands
       * */
      if (checkLocalFileCommandExists("finish " + jobName)) {
        handleFinishJobCommand();
        LOG.info("Find finish-job command in local command file " + commandFile + ", exit the job...");
        break;
      }
    } while (fetchManager.getActiveFetchThreadCount() > 0);
  }

  /**
   * Handle job timeout
   * */
  public int handleJobTimeout() { return fetchManager.clearReadyTasks(); }

  /**
   * Check if some threads are hung. If so, we should stop the main fetch loop
   * should we stop the main fetch loop
   * */
  private void handleFetchTaskTimeout() {
    int activeFetchThreads = fetchManager.getActiveFetchThreadCount();
    if (activeFetchThreads <= 0) {
      return;
    }

    LOG.warn("Aborting with " + activeFetchThreads + " hung threads");

    fetchManager.dumpFetchThreads();
  }

  private void handleFewFetchItems() {
    if (!isFeederAlive()) {
      fetchManager.dumpFetchItems(QUEUE_REMAINDER_LIMIT);
    }
  }

  private void handleFinishJobCommand() {
    // fetchManager.clearReadyTasks(false);
  }

  /**
   * Check local command file.
   * */
  private boolean checkLocalFileCommandExists(String command) {
    boolean exist = false;

    Path path = Paths.get(commandFile);
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
    return queueFeederThread != null && queueFeederThread.isAlive();
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

    boolean shouldCheck = now > pendingQueueLastCheckTime + 2 * reportIntervalSec * 1000;
    if (shouldCheck) {
      shouldCheck = idleTime > pendingTimeout || now - pendingQueueLastCheckTime > pendingQueueCheckInterval;
    }

    if (shouldCheck) {
      LOG.info("Checking pending items ...");
      fetchManager.reviewPendingTasks(false);
      pendingQueueLastCheckTime = now;
    }
  }

  /**
   * Check if we're dropping below the threshold (we are too slow)
   * */
  private void checkFetchEfficiency(Context context) throws IOException {
    lowThroughputCount++;
    totalLowThroughputCount++;

    int removedSlowTasks = 0;
    if (lowThroughputCount > maxLowThroughputCount) {
      // Clear slowest queues
      removedSlowTasks = fetchManager.clearSlowestFetchQueue();
      LOG.warn("Unaccepted throughput, clear slowest queue."
          + " lowThroughputCount : " + lowThroughputCount
          + ", maxLowThroughputCount : " + maxLowThroughputCount
          + ", minPageThroughputRate : " + minPageThroughputRate + " pages/sec"
          + ", removedSlowTasks : " + removedSlowTasks
      );

      if (removedSlowTasks > 0) {
        lowThroughputCount = 0;
      }
    }

    // Quit if we dropped below threshold too many times
    if (totalLowThroughputCount > maxTotalLowThroughputCount) {
      // Clear all queues
      removedSlowTasks = fetchManager.clearReadyTasks();
      LOG.warn("Unaccepted throughput, clear all queues."
              + " totalLowThroughputCount : " + totalLowThroughputCount
              + ", maxTotalLowThroughputCount : " + maxTotalLowThroughputCount
              + ", minPageThroughputRate : " + minPageThroughputRate + " pages/sec"
              + ", removedSlowTasks : " + removedSlowTasks
      );
      totalLowThroughputCount = 0;
    }

    if (removedSlowTasks > 0) {
      context.getCounter("Runtime Status", "RemovedSlowItems").increment(removedSlowTasks);
    }
  }
}
