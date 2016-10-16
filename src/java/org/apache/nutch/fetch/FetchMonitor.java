package org.apache.nutch.fetch;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchContext;
import org.apache.nutch.fetch.indexer.IndexThread;
import org.apache.nutch.fetch.indexer.JITIndexer;
import org.apache.nutch.fetch.service.FetchServer;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.net.proxy.ProxyUpdateThread;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.apache.nutch.fetch.TaskScheduler.QUEUE_REMAINDER_LIMIT;
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

  private long startTime = System.currentTimeMillis();

  private String jobName;
  private FetchMode fetchMode = FetchMode.NATIVE;

  /** Threads */
  private Integer fetchServerPort;
  private FetchServer fetchServer;
  private int fetchThreadCount = 5;

  /** Monitors */
  private TaskSchedulers taskSchedulers;
  private TaskScheduler taskScheduler;
  private TasksMonitor fetchMonitor;

  /** Timing */
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
  private long reportInterval;

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

    fetchJobTimeout = 60 * 1000 * NutchConfiguration.getUint(conf, "fetcher.timelimit.mins", Integer.MAX_VALUE / 60 / 1000 / 20);
    fetchTaskTimeout = conf.getTimeDuration("mapred.task.timeout", Duration.ofMinutes(10).toMillis(), TimeUnit.MILLISECONDS);
    // pendingQueueCheckInterval = 60 * 1000 * conf.getLong("fetcher.pending.queue.check.time", 8);
    pendingQueueCheckInterval = conf.getTimeDuration("fetcher.pending.queue.check.time", Duration.ofMinutes(8).toMillis(), TimeUnit.MILLISECONDS);
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

    // fetch scheduler
    taskSchedulers = TaskSchedulers.getInstance();
    taskScheduler = taskSchedulers.create(this, counter, context);
    fetchMonitor = taskScheduler.getTasksMonitor();

    // report
    reportInterval = conf.getTimeDuration("fetcher.report.interval", Duration.ofSeconds(20).toMillis(), TimeUnit.MILLISECONDS);

    // scripts
    Path nutchTmpDir = NutchConfiguration.getPath(conf, PARAM_NUTCH_TMP_DIR, Paths.get(PATH_NUTCH_TMP_DIR));
    commandFile = PATH_LOCAL_COMMAND;
    finishScript = nutchTmpDir + "/scripts/finish_" + jobName + ".sh";

    DecimalFormat df = new DecimalFormat("###0.0#");
    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "fetchMode", fetchMode,

        "fetchServerPort", fetchServerPort,

        "taskSchedulers", taskSchedulers.name(),
        "taskScheduler", taskScheduler.name(),

        "fetchJobTimeout(m)", df.format(fetchJobTimeout / 60.0 / 1000),
        "fetchTaskTimeout(m)", df.format(fetchTaskTimeout / 60.0 / 1000),
        "pendingTimeout(m)", df.format(pendingTimeout / 60.0 / 1000),
        "queueCheckInterval(m)", df.format(pendingQueueCheckInterval / 60.0 / 1000),
        "queueLastCheckTime", TimingUtil.format(pendingQueueLastCheckTime),

        "minPageThroughputRate", minPageThroughputRate,
        "maxLowThroughputCount", maxLowThroughputCount,
        "throughputCheckTimeLimit", TimingUtil.format(throughputCheckTimeLimit),

        "reportInterval(s)", df.format(reportInterval / 1000.0),

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

    startNutchMaster();

    if (!NutchServer.isRunning()) {
      LOG.error("Failed to start nutch master, exit ...");
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
      // LOG.info("Clean up reducer, job name : " + context.getJobName());

      if (fetchServer != null && fetchServer.isRunning()) {
        fetchServer.stop(true);
      }

      if (taskScheduler != null) {
        taskScheduler.cleanup();
        taskSchedulers.remove(taskScheduler.getId());
      }

      Files.delete(Paths.get(finishScript));
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

  private void startFetchService(final Configuration conf, final int port) {
    fetchServer = FetchServer.startInDaemonThread(conf, port);
  }

  private int tryAcquireFetchServerPort() {
    int port = FetchServer.acquirePort(conf);
    if (port < 0) {
      LOG.error("Failed to acquire fetch server port");
      return -1;
    }
    return port;
  }

  private void startNutchMaster() throws InterruptedException {
    if (!NutchServer.isRunning()) {
      NutchServer.startInDaemonThread(conf);
    }

    int counter = 10;
    while (counter-- > 0 && !NutchServer.isRunning()) {
      sleep(1000);
    }
  }

  private void startCheckAndReportLoop() throws IOException {
    do {
      taskScheduler.adjustFetchResourceByTargetBandwidth();

      TaskScheduler.Status status = taskScheduler.waitAndReport(reportInterval);

      long now = System.currentTimeMillis();
      long jobTime = now - startTime;
      long idleTime = now - taskScheduler.getLastTaskFinishTime();

      /**
       * In crowd sourcing mode, check if any fetch tasks are hung
       * */
      retuneFetchQueues(now, idleTime);

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
        checkFetchEfficiency();
      }

      /**
       * Halt command is received
       * */
      if (isHalt()) {
        LOG.info("Received halt command, exit the job ...");
        break;
      }

      /**
       * Read local filesystem for control commands
       * */
      if (checkLocalFileCommandExists("finish " + jobName)) {
        handleFinishJobCommand();
        LOG.info("Find finish-job command in " + commandFile + ", exit the job ...");
        halt();
        break;
      }

      /**
       * All fetch tasks are finished
       * */
      if (taskScheduler.isMissionComplete()) {
        LOG.info("All done, exit the job ...");
        break;
      }

      if (jobTime > fetchJobTimeout) {
        handleJobTimeout();
        LOG.info("Hit fetch job timeout " + jobTime / 1000 + "s, exit the job ...");
        break;
      }

      /**
       * No new tasks for too long, some requests seem to hang. We exits the job.
       * */
      if (idleTime > fetchTaskTimeout) {
        handleFetchTaskTimeout();
        LOG.info("Hit fetch task timeout " + idleTime / 1000 + "s, exit the job ...");
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
      fetchMonitor.dump(QUEUE_REMAINDER_LIMIT);
    }
  }

  private void handleFinishJobCommand() { fetchMonitor.clearReadyTasks(); }

  /**
   * Check local command file
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
   * Check queues to see if something is hung
   * */
  private void retuneFetchQueues(long now, long idleTime) {
    // TODO : use a smarter schedule strategy
    if (fetchMonitor.readyItemCount() + fetchMonitor.pendingItemCount() < 10) {
      pendingTimeout = 2 * 60 * 1000;
    }

    boolean shouldRetune = now > pendingQueueLastCheckTime + 2 * reportInterval;
    if (shouldRetune) {
      shouldRetune = idleTime > pendingTimeout || now - pendingQueueLastCheckTime > pendingQueueCheckInterval;
    }

    if (shouldRetune) {
      fetchMonitor.retune(false);
      pendingQueueLastCheckTime = now;
    }
  }

  /**
   * Check if we're dropping below the threshold (we are too slow)
   * */
  private void checkFetchEfficiency() throws IOException {
    lowThroughputCount++;
    totalLowThroughputCount++;

    int removedSlowTasks = 0;
    if (lowThroughputCount > maxLowThroughputCount) {
      // Clear slowest queues
      removedSlowTasks = fetchMonitor.clearSlowestQueue();

      LOG.info(Params.formatAsLine(
          "Unaccepted throughput", "clearing slowest queue, ",
          "lowThoCount", lowThroughputCount,
          "maxLowThoCount", maxLowThroughputCount,
          "minPageThoRate(p/s)", minPageThroughputRate,
          "removedSlowTasks", removedSlowTasks
      ));

      lowThroughputCount = 0;
    }

    // Quit if we dropped below threshold too many times
    if (totalLowThroughputCount > maxTotalLowThroughputCount) {
      // Clear all queues
      removedSlowTasks = fetchMonitor.clearReadyTasks();
      LOG.info(Params.formatAsLine(
          "Unaccepted throughput", "all queues cleared",
          "lowThoCount", lowThroughputCount,
          "maxLowThoCount", maxLowThroughputCount,
          "minPageThoRate(p/s)", minPageThroughputRate,
          "removedSlowTasks", removedSlowTasks
      ));

      totalLowThroughputCount = 0;
    }

    if (removedSlowTasks > 0) {
      // context.getCounter("Runtime Status", "RemovedSlowItems").increment(removedSlowTasks);
    }
  }
}
