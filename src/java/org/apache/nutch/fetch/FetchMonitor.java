package org.apache.nutch.fetch;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.common.Params;
import org.apache.nutch.crawl.NutchContext;
import org.apache.nutch.fetch.indexer.IndexThread;
import org.apache.nutch.fetch.indexer.JITIndexer;
import org.apache.nutch.fetch.service.FetchServer;
import org.apache.nutch.fetch.service.FetchServerThread;
import org.apache.nutch.jobs.NutchCounter;
import org.apache.nutch.net.proxy.ProxyUpdateThread;
import org.apache.nutch.service.NutchMaster;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * Created by vincent on 16-9-24.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class FetchMonitor {

  public static final Logger LOG = LoggerFactory.getLogger(FetchMonitor.class);
  public static final Logger REPORT_LOG = NutchMetrics.REPORT_LOG;

  private Configuration conf;
  private NutchContext context;

  private Instant startTime = Instant.now();

  private String jobName;
  private FetchMode fetchMode = FetchMode.NATIVE;

  /** Threads */
  private Integer fetchServerPort;
  private FetchServerThread fetchServerThread;
  private int fetchThreadCount = 5;

  /** Monitors */
  private TaskSchedulers taskSchedulers;
  private TaskScheduler taskScheduler;
  private TasksMonitor fetchMonitor;

  /** Index server */
  private final String indexServer = "master";
  private final int indexServerPort = 8983;

  /** Timing */
  private Duration fetchJobTimeout;
  private Duration fetchTaskTimeout;
  private Duration queueRetuneInterval;
  private Duration queuePendingTimeout;
  private Instant queueRetuneTime;

  /** Throughput control */
  private int minPageThoRate;
  private Instant thoCheckTime;
  private int maxLowThoCount;
  private int maxTotalLowThoCount;
  private int lowThoCount = 0;
  private int totalLowThoCount = 0;

  /** Report */
  private Duration reportInterval;

  /** Scripts */
  private String finishScript;
  private String commandFile;

  private boolean halt = false;

  public FetchMonitor(String jobName, NutchCounter counter, NutchContext context) throws IOException {
    this.conf = context.getConfiguration();
    this.context = context;
    this.jobName = jobName;

    String crawlId = conf.get(PARAM_CRAWL_ID);

    fetchMode = conf.getEnum(PARAM_FETCH_MODE, FetchMode.NATIVE);
    fetchThreadCount = conf.getInt("fetcher.threads.fetch", 5);

    fetchJobTimeout = ConfigUtils.getDuration(conf, "fetcher.timelimit", Duration.ofHours(1));
    fetchTaskTimeout = ConfigUtils.getDuration(conf, "fetcher.task.timeout", Duration.ofMinutes(10));
    queueRetuneInterval = ConfigUtils.getDuration(conf, PARAM_FETCH_QUEUE_RETUNE_INTERVAL, Duration.ofMinutes(8));
    queuePendingTimeout = ConfigUtils.getDuration(conf, "fetcher.pending.timeout", queueRetuneInterval.multipliedBy(2));
    queueRetuneTime = startTime;

    /*
     * Used for threshold check, holds pages and bytes processed in the last sec
     * We should keep a minimal fetch speed
     * */
    minPageThoRate = conf.getInt("fetcher.throughput.threshold.pages", -1);
    maxLowThoCount = conf.getInt("fetcher.throughput.threshold.sequence", 10);
    maxTotalLowThoCount = maxLowThoCount * 10;
    long thoCheckAfter = conf.getLong("fetcher.throughput.threshold.check.after", 30);
    thoCheckTime = startTime.plus(thoCheckAfter, ChronoUnit.SECONDS);

    // fetch scheduler
    taskSchedulers = TaskSchedulers.getInstance();
    taskScheduler = taskSchedulers.create(this, counter, context);
    fetchMonitor = taskScheduler.getTasksMonitor();

    // report
    reportInterval = ConfigUtils.getDuration(conf, PARAM_FETCH_REPORT_INTERVAL, Duration.ofSeconds(20));

    // scripts
    Path nutchTmpDir = ConfigUtils.getPath(conf, PARAM_NUTCH_TMP_DIR, Paths.get(PATH_NUTCH_TMP_DIR));
    commandFile = PATH_LOCAL_COMMAND;
    finishScript = nutchTmpDir + "/scripts/finish_" + jobName + ".sh";

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "fetchMode", fetchMode,

        "fetchServerPort", fetchServerPort,

        "taskSchedulers", taskSchedulers.name(),
        "taskScheduler", taskScheduler.name(),

        "fetchJobTimeout(m)", fetchJobTimeout.toMinutes(),
        "fetchTaskTimeout(m)", fetchTaskTimeout.toMinutes(),
        "queuePendingTimeout(m)", queuePendingTimeout.toMinutes(),
        "queueCheckInterval(m)", queueRetuneInterval.toMinutes(),
        "queueLastCheckTime", DateTimeUtil.format(queueRetuneTime),

        "minPageThoRate", minPageThoRate,
        "maxLowThoCount", maxLowThoCount,
        "thoCheckTime", DateTimeUtil.format(thoCheckTime),

        "reportInterval(s)", reportInterval.getSeconds(),

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

  public void run() throws IOException, InterruptedException {
    // Queue feeder thread
    startFeederThread(context);

    if (!NutchMaster.isRunning(conf)) {
      LOG.error("Failed to find nutch master, exit ...");
      return;
    }

    fetchServerPort = tryAcquireFetchServerPort();
    startFetchService(conf, fetchServerPort);

    if (FetchMode.CROWDSOURCING.equals(fetchMode)) {
      startCrowdsourcingThreads();
    }
    else {
      if (FetchMode.PROXY.equals(fetchMode)) {
        ProxyUpdateThread proxyUpdateThread = new ProxyUpdateThread(conf);
        proxyUpdateThread.start();
      }

      // Threads for native or proxy mode
      startNativeFetcherThreads();
    }

    if (taskScheduler.indexJIT()) {
      startIndexThreads();
    }

    startCheckAndReportLoop();
  }

  public void cleanup() {
    try {
      LOG.info("Clean up FetchMonitor");
      fetchServerThread.getServer().stop(true);

      if (taskScheduler != null) {
        taskScheduler.cleanup();
        taskSchedulers.remove(taskScheduler.getId());
      }

      if (Files.exists(Paths.get(finishScript))) {
        Files.delete(Paths.get(finishScript));
      }
    }
    catch (Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
  }

  public void halt() {
    halt = true;
  }

  public boolean isHalt() {
    return halt;
  }

  /**
   * Start queue feeder thread. The thread fetches webpages from the reduce result
   * and add it into the fetch queue
   * Non-Blocking
   * @throws InterruptedException
   * @throws IOException
   * */
  private void startFeederThread(NutchContext context) throws IOException, InterruptedException {
    FeederThread feederThread = new FeederThread(taskScheduler, context);
    feederThread.start();
  }

  /**
   * Start crowd sourcing threads
   * Non-Blocking
   * */
  private void startCrowdsourcingThreads() {
    startFetchThreads(fetchThreadCount);
  }

  /**
   * Start native fetcher threads
   * Non-Blocking
   * */
  private void startNativeFetcherThreads() {
    startFetchThreads(fetchThreadCount);
  }

  private void startFetchThreads(int threadCount) {
    for (int i = 0; i < threadCount; i++) {
      FetchThread fetchThread = new FetchThread(taskScheduler, conf);
      fetchThread.start();
    }
  }

  /**
   * Start index threads
   * Non-Blocking
   * */
  private void startIndexThreads() throws IOException {
    JITIndexer JITIndexer = taskScheduler.getJitIndexer();
    if (JITIndexer == null) {
      LOG.error("Unexpected JITIndexer, should not be null");
      return;
    }

    for (int i = 0; i < JITIndexer.getIndexThreadCount(); i++) {
      IndexThread indexThread = new IndexThread(JITIndexer, conf);
      indexThread.start();
    }
  }

  private void startFetchService(Configuration conf, int port) {
    fetchServerThread = new FetchServerThread(conf, port);
    fetchServerThread.start();
  }

  private int tryAcquireFetchServerPort() {
    int port = FetchServer.acquirePort(conf);
    if (port < 0) {
      LOG.error("Failed to acquire fetch server port");
      return -1;
    }
    return port;
  }

  private void startCheckAndReportLoop() throws IOException {
    do {
      taskScheduler.adjustFetchResourceByTargetBandwidth();

      TaskScheduler.Status status = taskScheduler.waitAndReport(reportInterval);

      Instant now = Instant.now();
      Duration jobTime = Duration.between(startTime, now);
      Duration idleTime = Duration.between(taskScheduler.getLastTaskFinishTime(), now);

      /*
       * In crowd sourcing mode, check if any fetch tasks are hung
       * */
      retuneFetchQueues(now, idleTime);

      /*
       * Dump the remainder fetch items if feeder thread is no available and fetch item is few
       * */
      if (fetchMonitor.taskCount() <= FETCH_TASK_REMAINDER_NUMBER) {
        LOG.info("Totally remains only " + fetchMonitor.taskCount() + " tasks");
        handleFewFetchItems();
      }

      /*
       * Check throughput(fetch speed)
       * */
      if (now.isAfter(thoCheckTime) && status.pagesThoRate < minPageThoRate) {
        checkFetchThroughput();
      }

      /*
       * Halt command is received
       * */
      if (isHalt()) {
        LOG.info("Received halt command, exit the job ...");
        break;
      }

      /*
       * Read local filesystem for control commands
       * */
      if (RuntimeUtil.checkLocalFileCommand(commandFile, "finish " + jobName)) {
        handleFinishJobCommand();
        LOG.info("Find finish-job command in " + commandFile + ", exit the job ...");
        halt();
        break;
      }

      /*
       * All fetch tasks are finished
       * */
      if (taskScheduler.isMissionComplete()) {
        LOG.info("All done, exit the job ...");
        break;
      }

      if (jobTime.getSeconds() > fetchJobTimeout.getSeconds()) {
        handleJobTimeout();
        LOG.info("Hit fetch job timeout " + jobTime.getSeconds() + "s, exit the job ...");
        break;
      }

      /*
       * No new tasks for too long, some requests seem to hang. We exits the job.
       * */
      if (idleTime.getSeconds() > fetchTaskTimeout.getSeconds()) {
        handleFetchTaskTimeout();
        LOG.info("Hit fetch task timeout " + idleTime.getSeconds() + "s, exit the job ...");
        break;
      }

      if (!NetUtil.testHttpNetwork(indexServer, indexServerPort)) {
        LOG.info("Lost index server, exit the job");
        break;
      }
    } while (taskScheduler.getActiveFetchThreadCount() > 0);
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
    int activeFetchThreads = taskScheduler.getActiveFetchThreadCount();
    if (activeFetchThreads <= 0) {
      return;
    }

    LOG.warn("Aborting with " + activeFetchThreads + " hung threads");

    taskScheduler.dumpFetchThreads();
  }

  private void handleFewFetchItems() {
    if (!taskScheduler.isFeederAlive()) {
      fetchMonitor.dump(FETCH_TASK_REMAINDER_NUMBER);
    }
  }

  private void handleFinishJobCommand() { fetchMonitor.clearReadyTasks(); }

  /**
   * Check queues to see if something is hung
   * */
  private void retuneFetchQueues(Instant now, Duration idleTime) {
    if (fetchMonitor.readyTaskCount() + fetchMonitor.pendingTaskCount() < 20) {
      queuePendingTimeout = Duration.ofMinutes(2);
    }

    // Do not check in every report loop
    Instant nextCheckTime = queueRetuneTime.plus(reportInterval.multipliedBy(2));
    if (now.isAfter(nextCheckTime)) {
      Instant nextRetuneTime = queueRetuneTime.plus(queueRetuneInterval);
      if (now.isAfter(nextRetuneTime) || idleTime.compareTo(queuePendingTimeout) > 0) {
        fetchMonitor.retune(false);
        queueRetuneTime = now;
      }
    }
  }

  /**
   * Check if we're dropping below the threshold (we are too slow)
   * */
  private void checkFetchThroughput() throws IOException {
    lowThoCount++;
    totalLowThoCount++;

    int removedSlowTasks = 0;
    if (lowThoCount > maxLowThoCount) {
      // Clear slowest queues
      removedSlowTasks = fetchMonitor.clearSlowestQueue();

      LOG.info(Params.formatAsLine(
          "Unaccepted throughput", "clearing slowest queue, ",
          "lowThoCount", lowThoCount,
          "maxLowThoCount", maxLowThoCount,
          "minPageThoRate(p/s)", minPageThoRate,
          "removedSlowTasks", removedSlowTasks
      ));

      lowThoCount = 0;
    }

    // Quit if we dropped below threshold too many times
    if (totalLowThoCount > maxTotalLowThoCount) {
      // Clear all queues
      removedSlowTasks = fetchMonitor.clearReadyTasks();
      LOG.info(Params.formatAsLine(
          "Unaccepted throughput", "all queues cleared",
          "lowThoCount", lowThoCount,
          "maxLowThoCount", maxLowThoCount,
          "minPageThoRate(p/s)", minPageThoRate,
          "removedSlowTasks", removedSlowTasks
      ));

      totalLowThoCount = 0;
    }

    if (removedSlowTasks > 0) {
      // context.getCounter("Runtime Status", "RemovedSlowItems").increment(removedSlowTasks);
    }
  }
}
