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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.fetcher.indexer.JITIndexer;
import org.apache.nutch.fetcher.indexer.IndexThread;
import org.apache.nutch.fetcher.service.FetchServer;
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
import java.text.DecimalFormat;
import java.util.List;

import static org.apache.nutch.fetcher.FetchScheduler.QUEUE_REMAINDER_LIMIT;

class FetchReducer extends NutchReducer<IntWritable, FetchEntry, String, WebPage> {

  public static final Logger LOG = FetchJob.LOG;

  private String jobName;
  private FetchMode fetchMode = FetchMode.NATIVE;

  /** Threads */
  private Integer fetchServerPort;
  private FetchServer fetchServer;
  private int fetchThreadCount = 5;

  /** Monitors */
  private FetchSchedulers fetchSchedulers;
  private FetchScheduler fetchScheduler;
  private FetchMonitor fetchMonitor;

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

  /** Report */
  private int reportIntervalSec;

  /** Scripts */
  private String finishScript;
  private String commandFile;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(FetchScheduler.Counter.class);
    getReporter().silence();

    jobName = context.getJobName();

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);

    fetchMode = FetchMode.fromString(conf.get(Nutch.FETCH_MODE_KEY, FetchMode.NATIVE.value()));
    if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
      fetchServerPort = tryAcquireFetchServerPort();
    }
    fetchThreadCount = conf.getInt("fetcher.threads.fetch", 5);

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

    // fetch manager
    fetchSchedulers = FetchSchedulers.getInstance();
    fetchScheduler = fetchSchedulers.create(getCounter(), context);
    fetchMonitor = fetchScheduler.getFetchMonitor();

    // report
    reportIntervalSec = conf.getInt("fetcher.pending.timeout.secs", 20);

    // scripts
    String nutchTmpDir = conf.get("nutch.tmp.dir", Nutch.NUTCH_TMP_DIR);
    commandFile = Nutch.NUTCH_LOCAL_COMMAND_FILE;
    finishScript = nutchTmpDir + "/scripts/finish_" + jobName + ".sh";

    DecimalFormat df = new DecimalFormat("###0.0#");
    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "fetchMode", fetchMode,

        "fetchServerPort", fetchServerPort,

        "fetchSchedulers", fetchSchedulers.name(),
        "fetchScheduler", fetchScheduler.name(),

        "fetchJobTimeout(m)", df.format(fetchJobTimeout / 60.0 / 1000),
        "fetchTaskTimeout(m)", df.format(fetchTaskTimeout / 60.0 / 1000),
        "pendingQueueCheckInterval(m)", df.format(pendingQueueCheckInterval / 60.0 / 1000),
        "pendingTimeout(m)", df.format(pendingTimeout / 60.0 / 1000),
        "pendingQueueLastCheckTime", TimingUtil.format(pendingQueueLastCheckTime),

        "minPageThroughputRate", minPageThroughputRate,
        "maxLowThroughputCount", maxLowThroughputCount,
        "throughputCheckTimeLimit", TimingUtil.format(throughputCheckTimeLimit),

        "reportIntervalSec(s)", reportIntervalSec,

        "nutchTmpDir", nutchTmpDir,
        "finishScript", finishScript
    ));

    generateFinishCommand();
  }

  private void generateFinishCommand() {
    String cmd = "#bin\necho finish " + jobName + " >> " + commandFile;

    Path script = Paths.get(finishScript);
    try {
      Files.createDirectories(script.getParent());
      Files.write(script, cmd.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwxrw-r--"));
    } catch (IOException e) {
      LOG.error(e.toString());
    }
  }

  @Override
  protected void doRun(Context context) throws IOException, InterruptedException {
    // Queue feeder thread
    startFeederThread(context);

    if (FetchMode.CROWDSOURCING.equals(fetchMode)) {
      startCrowdsourcingThreads(context);

      startFetchService(conf, fetchServerPort);
    }
    else {
      if (FetchMode.PROXY.equals(fetchMode)) {
        ProxyUpdateThread proxyUpdateThread = new ProxyUpdateThread(conf);
        proxyUpdateThread.start();
      }

      // Threads for native or proxy mode
      startNativeFetcherThreads(context);
    }

    if (fetchScheduler.indexJIT()) {
      startIndexThreads(context);
    }

    startCheckAndReportLoop(context);
  }

  @Override
  protected void cleanup(Context context) {
    try {
      // LOG.info("Clean up reducer, job name : " + context.getJobName());

      if (fetchServer != null && fetchServer.isRunning()) {
        fetchServer.stop(true);
      }

      if (fetchScheduler != null) {
        fetchScheduler.cleanup();
        fetchSchedulers.remove(fetchScheduler.getId());
      }

      Files.delete(Paths.get(finishScript));
    }
    catch (Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
    finally {
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
  private void startFeederThread(Context context) throws IOException, InterruptedException {
    FeederThread feederThread = new FeederThread(fetchScheduler, context);
    feederThread.start();
  }

  /**
   * Start crowd sourcing threads
   * Non-Blocking
   * */
  private void startCrowdsourcingThreads(Context context) {
    startFetchThreads(fetchThreadCount, context);
  }

  /**
   * Start native fetcher threads
   * Non-Blocking
   * */
  private void startNativeFetcherThreads(Context context) {
    startFetchThreads(fetchThreadCount, context);
  }

  private void startFetchThreads(int threadCount, Context context) {
    for (int i = 0; i < threadCount; i++) {
      FetchThread fetchThread = new FetchThread(fetchScheduler, context);
      fetchThread.start();
    }
  }

  /**
   * Start index threads
   * Non-Blocking
   * */
  public void startIndexThreads(Reducer.Context context) throws IOException {
    JITIndexer JITIndexer = fetchScheduler.getJITIndexer();
    if (JITIndexer == null) {
      LOG.error("Unexpected JITIndexer, should not be null");
      return;
    }

    for (int i = 0; i < JITIndexer.getIndexThreadCount(); i++) {
      IndexThread indexThread = new IndexThread(JITIndexer, context);
      indexThread.start();
    }
  }

  private void startFetchService(final Configuration conf, final int port) {
    fetchServer = FetchServer.startInDaemonThread(conf, port);
  }

  private Integer tryAcquireFetchServerPort() {
    int port = FetchServer.acquirePort(conf);
    if (port < 0) {
      LOG.error("Failed to acquire fetch server port");
      stop();
    }
    return port;
  }

  private void startCheckAndReportLoop(Context context) throws IOException {
    do {
      fetchScheduler.adjustFetchResourceByTargetBandwidth();

      FetchScheduler.Status status = fetchScheduler.waitAndReport(reportIntervalSec);

      long now = System.currentTimeMillis();
      long jobTime = now - startTime;
      long idleTime = now - fetchScheduler.getLastTaskFinishTime();

      /**
       * In crowd sourcing mode, check if any fetch tasks are hung
       * */
      checkPendingQueue(now, idleTime);

      /**
       * Dump the remainder fetch items if feeder thread is no available and fetch item is few
       * */
      if (fetchMonitor.getQueueCount() <= QUEUE_REMAINDER_LIMIT) {
        handleFewFetchItems();
      }

      /**
       * Check throughput(fetch speed)
       * */
      if (now > throughputCheckTimeLimit && status.pagesThroughputRate < minPageThroughputRate) {
        checkFetchEfficiency(context);
      }

      /**
       * All fetch tasks are finished
       * */
      if (fetchScheduler.isMissionComplete()) {
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
    } while (fetchScheduler.getActiveFetchThreadCount() > 0);
  }

  /**
   * Handle job timeout
   * */
  private int handleJobTimeout() { return fetchMonitor.clearReadyTasks(); }

  /**
   * Check if some threads are hung. If so, we should stop the main fetch loop
   * should we stop the main fetch loop
   * */
  private void handleFetchTaskTimeout() {
    int activeFetchThreads = fetchScheduler.getActiveFetchThreadCount();
    if (activeFetchThreads <= 0) {
      return;
    }

    LOG.warn("Aborting with " + activeFetchThreads + " hung threads");

    fetchScheduler.dumpFetchThreads();
  }

  private void handleFewFetchItems() {
    if (!fetchScheduler.isFeederAlive()) {
      fetchMonitor.dump(QUEUE_REMAINDER_LIMIT);
    }
  }

  private void handleFinishJobCommand() {
    fetchMonitor.clearReadyTasks();
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
        LOG.error(e.toString());
      }
    }

    return exist;
  }

  /**
   * Check pending queue to see if some item is expired,
   * which often means the fetch client running into wrong
   * */
  private void checkPendingQueue(long now, long idleTime) {
    if (fetchMonitor.readyItemCount() + fetchMonitor.pendingItemCount() < 10) {
      pendingTimeout = 2 * 60 * 1000;
    }

    boolean shouldCheck = now > pendingQueueLastCheckTime + 2 * reportIntervalSec * 1000;
    if (shouldCheck) {
      shouldCheck = idleTime > pendingTimeout || now - pendingQueueLastCheckTime > pendingQueueCheckInterval;
    }

    if (shouldCheck) {
      LOG.info("Checking pending items ...");
      fetchMonitor.review(false);
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
      removedSlowTasks = fetchMonitor.clearSlowestQueue();
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
      removedSlowTasks = fetchMonitor.clearReadyTasks();
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
