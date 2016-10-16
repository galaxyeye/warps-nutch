package org.apache.nutch.fetch;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.NutchContext;
import org.apache.nutch.crawl.SeedBuilder;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.crawl.filters.CrawlFilter;
import org.apache.nutch.dbupdate.MapDatumBuilder;
import org.apache.nutch.dbupdate.ReduceDatumBuilder;
import org.apache.nutch.fetch.data.FetchTask;
import org.apache.nutch.fetch.indexer.JITIndexer;
import org.apache.nutch.fetch.service.FetchResult;
import org.apache.nutch.mapreduce.*;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nutch.metadata.Nutch.*;

public class TaskScheduler extends Configured {

  private final Logger LOG = FetchMonitor.LOG;
  public static final Logger REPORT_LOG = NutchMetrics.REPORT_LOG;

  public class Status {
    Status(float pagesThroughputRate, float bytesThroughputRate, int readyFetchItems, int pendingFetchItems) {
      this.pagesThroughputRate = pagesThroughputRate;
      this.bytesThroughputRate = bytesThroughputRate;
      this.readyFetchItems = readyFetchItems;
      this.pendingFetchItems = pendingFetchItems;
    }

    public float pagesThroughputRate;
    public float bytesThroughputRate;
    public int readyFetchItems;
    public int pendingFetchItems;
  }

  public enum Counter {
    mbytes, unknowHosts,
    readyTasks, pendingTasks,
    pagesThou, mbThou,
    rowsRedirect,
    rowsPeresist, newRowsPeresist, newDetailRows
  }

  public static final int QUEUE_REMAINDER_LIMIT = 5;

  private static final AtomicInteger objectSequence = new AtomicInteger(0);

  private final int id;

  private final FetchMonitor fetchMonitor;

  private final NutchContext context;
  private final NutchCounter counter;

  private final TasksMonitor tasksMonitor;// all fetch items are contained in several queues
  private final Queue<FetchResult> fetchResultQueue = new ConcurrentLinkedQueue<>();

  /**
   * Our own Hardware bandwidth in Mbytes, if exceed the limit, slows down the task scheduling.
   * TODO : automatically adjust
   */
  private final int bandwidth;

  // Handle redirect
  private final URLFilters urlFilters;
  private final URLNormalizers normalizers;
  private final boolean ignoreExternalLinks;

  // Parser setting
  private final boolean storingContent;
  private final boolean parse;
  private final ParseUtil parseUtil;
  private final boolean skipTruncated;

  /** Injector */
  private final SeedBuilder seedBuiler;

  /** Feeder threads */
  private final int maxFeedPerThread;
  private final Set<FeederThread> feederThreads = new ConcurrentSkipListSet<>();

  /** Fetch threads */
  private final int initFetchThreadCount;
  private final int maxThreadsPerQueue;

  private final Set<FetchThread> activeFetchThreads = new ConcurrentSkipListSet<>();
  private final Set<FetchThread> retiredFetchThreads = new ConcurrentSkipListSet<>();
  private final Set<FetchThread> idleFetchThreads = new ConcurrentSkipListSet<>();
  private final AtomicInteger activeFetchThreadCount = new AtomicInteger(0);
  private final AtomicInteger idleFetchThreadCount = new AtomicInteger(0);

  /** DbUpdate */
  private final int maxDbUpdateNewRows;

  /** Indexer */
  private final JITIndexer jitIndexer;

  /** Update */
  private final MapDatumBuilder mapDatumBuilder;
  private final ReduceDatumBuilder reduceDatumBuilder;

  // Timer
  private final long startTime = System.currentTimeMillis(); // Start time of fetcher run
  private final AtomicLong lastTaskStartTime = new AtomicLong(startTime);
  private final AtomicLong lastTaskFinishTime = new AtomicLong(startTime);

  // Statistics
  private final AtomicLong totalBytes = new AtomicLong(0);        // total fetched bytes
  private final AtomicInteger totalPages = new AtomicInteger(0);  // total fetched pages
  private final AtomicInteger fetchErrors = new AtomicInteger(0); // total fetch fetchErrors

  private final AtomicDouble avePageLength = new AtomicDouble(0.0);

  /** Output */
  private final Path outputDir;

  private final String reportSuffix;

  private final NutchMetrics nutchMetrics;

  /**
   * The reprUrl is the representative url of a redirect, we save a reprUrl for each thread,
   * We use a concurrent skip list map to gain the best concurrency
   * */
  // TODO : check why we store a reprUrl for each thread? @vincent
  private Map<Long, String> reprUrls = new ConcurrentSkipListMap<>();

  @SuppressWarnings("rawtypes")
  public TaskScheduler(FetchMonitor fetchMonitor, NutchCounter counter, NutchContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    setConf(conf);

    // TODO : use FetchMonitor.id
    this.id = objectSequence.incrementAndGet();

    this.fetchMonitor = fetchMonitor;

    this.counter = counter;
    this.context = context;

    this.bandwidth = 1024 * 1024 * conf.getInt("fetcher.net.bandwidth.m", Integer.MAX_VALUE);
    this.initFetchThreadCount = conf.getInt("fetcher.threads.fetch", 10);
    this.maxThreadsPerQueue = conf.getInt("fetcher.threads.per.queue", 1);
    this.maxFeedPerThread = conf.getInt("fetcher.queue.depth.multiplier", 100);

    this.urlFilters = new URLFilters(conf);
    this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);

    this.tasksMonitor = new TasksMonitor(conf);
    this.seedBuiler = new SeedBuilder(conf);

    this.maxDbUpdateNewRows = conf.getInt("db.update.max.outlinks", 100);

    // Index manager
    boolean indexJIT = conf.getBoolean(Nutch.PARAM_INDEX_JUST_IN_TIME, false);
    this.jitIndexer = indexJIT ? new JITIndexer(conf) : null;

    boolean updateJIT = getConf().getBoolean(PARAM_DBUPDATE_JUST_IN_TIME, true);
    mapDatumBuilder = updateJIT ? new MapDatumBuilder(counter, conf) : null;
    reduceDatumBuilder = updateJIT ? new ReduceDatumBuilder(counter, conf) : null;

    this.parse = indexJIT || conf.getBoolean(PARAM_PARSE, false);
    this.parseUtil = parse ? new ParseUtil(getConf()) : null;
    this.skipTruncated = getConf().getBoolean(ParserJob.SKIP_TRUNCATED, true);
    this.ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    this.storingContent = conf.getBoolean("fetcher.store.content", true);

    this.outputDir = NutchConfiguration.getPath(conf, PARAM_NUTCH_OUTPUT_DIR, Paths.get(PATH_NUTCH_OUTPUT_DIR));

    this.reportSuffix = conf.get(PARAM_NUTCH_JOB_NAME, "job-unknown-" + TimingUtil.now("MMdd.hhmm"));

    this.nutchMetrics = new NutchMetrics(conf);

    LOG.info(Params.format(
        "id", id,

        "bandwidth", bandwidth,
        "initFetchThreadCount", initFetchThreadCount,
        "maxThreadsPerQueue", maxThreadsPerQueue,
        "maxFeedPerThread", maxFeedPerThread,

        "skipTruncated", skipTruncated,
        "parse", parse,
        "storingContent", storingContent,
        "ignoreExternalLinks", ignoreExternalLinks,

        "indexJIT", indexJIT(),
        "updateJIT", updateJIT(),
        "outputDir", outputDir
    ));
  }

  public int getId() { return id; }

  public String name() { return getClass().getSimpleName() + "-" + id; }

  public int getBandwidth() { return this.bandwidth; }

  public FetchMonitor getFetchMonitor() {
    return fetchMonitor;
  }

  public TasksMonitor getTasksMonitor() {
    return tasksMonitor;
  }

  public JITIndexer getJitIndexer() {
    return jitIndexer;
  }

  public long getLastTaskFinishTime() {
    return lastTaskFinishTime.get();
  }

  public int getActiveFetchThreadCount() { return activeFetchThreadCount.get(); }

  public int getFeedLimit() {
    return initFetchThreadCount * maxFeedPerThread;
  }

  public boolean indexJIT() { return jitIndexer != null; }

  public boolean updateJIT() { return mapDatumBuilder != null && reduceDatumBuilder != null; }

  public void registerFeederThread(FeederThread feederThread) {
    feederThreads.add(feederThread);
  }

  public void unregisterFeederThread(FeederThread feederThread) {
    feederThreads.remove(feederThread);
  }

  public void registerFetchThread(FetchThread fetchThread) {
    activeFetchThreads.add(fetchThread);
    activeFetchThreadCount.incrementAndGet();
  }

  public void unregisterFetchThread(FetchThread fetchThread) {
    activeFetchThreads.remove(fetchThread);
    activeFetchThreadCount.decrementAndGet();

    retiredFetchThreads.add(fetchThread);
  }

  public void registerIdleThread(FetchThread thread) {
    idleFetchThreads.add(thread);
    idleFetchThreadCount.incrementAndGet();
  }

  public void unregisterIdleThread(FetchThread thread) {
    idleFetchThreads.remove(thread);
    idleFetchThreadCount.decrementAndGet();
  }

  /**
   * Return Injected urls count
   * */
  public int inject(Set<String> urlLines, String batchId) {
    return urlLines.stream().mapToInt(urlLine -> inject(urlLine, batchId)).sum();
  }

  /**
   * @return Injected urls count
   * */
  public int inject(String urlLine, String batchId) {
    if (urlLine == null || urlLine.isEmpty()) {
      return 0;
    }

    urlLine = urlLine.trim();
    if (urlLine.startsWith("#")) {
      return 0;
    }

    WebPage page = seedBuiler.buildWebPage(urlLine);
    String url = page.getVariableAsString("url");
    TableUtil.setPriority(page, FETCH_PRIORITY_SEED);
    // set score?

    Mark.INJECT_MARK.removeMarkIfExist(page);
    Mark.GENERATE_MARK.putMark(page, new Utf8(batchId));
    page.setBatchId(batchId);

    tasksMonitor.produce(context.getJobId(), url, page);

    LOG.info("Injected " + url + " with batch id " + batchId);

    return 1;
  }

  public void produce(FetchResult result) {
    fetchResultQueue.add(result);
  }

  public List<FetchTask> schedule(int number) {
    return schedule(null, number);
  }

  /**
   * Multiple threaded
   * */
  public FetchTask schedule(String queueId) {
    List<FetchTask> fetchTasks = schedule(queueId, 1);
    return fetchTasks.isEmpty() ? null : fetchTasks.iterator().next();
  }

  /**
   * Consume a fetch item and try to download the target web page
   */
  public List<FetchTask> schedule(String queueId, int number) {
    List<FetchTask> fetchTasks = Lists.newArrayList();
    if (number <= 0) {
      LOG.warn("Required no fetch item");
      return fetchTasks;
    }

    if (tasksMonitor.pendingItemCount() * avePageLength.get() * 8 > 30 * this.getBandwidth()) {
      LOG.info("Bandwidth exhausted, slows down scheduling");
      return fetchTasks;
    }

    while (number-- > 0) {
      FetchTask fetchTask = tasksMonitor.consume(queueId);
      if (fetchTask != null) fetchTasks.add(fetchTask);
    }

    if (!fetchTasks.isEmpty()) {
      lastTaskStartTime.set(System.currentTimeMillis());
    }

    return fetchTasks;
  }

  /**
   * Finish the fetch item anyway, even if it's failed to download the target page
   */
  public void finishUnchecked(FetchTask fetchTask) {
    tasksMonitor.finish(fetchTask);
    lastTaskFinishTime.set(System.currentTimeMillis());
  }

  /**
   * Finished downloading the web page
   *
   * Multiple threaded, non-synchronized class member variables are not allowed inside this method.
   */
  public void finish(String queueID, int itemID, ProtocolOutput output) {
    FetchTask fetchTask = tasksMonitor.getPendingTask(queueID, itemID);

    if (fetchTask == null) {
      LOG.warn("Failed to finish task [{} - {}]", queueID, itemID);

      return;
    }

    try {
      doFinishFetchTask(fetchTask, output);
    } catch (final Throwable t) {
      LOG.error("Unexpected error for " + fetchTask.getUrl(), t);

      tasksMonitor.finish(fetchTask);
      fetchErrors.incrementAndGet();

      try {
        handleResult(fetchTask, null, ProtocolStatusUtils.STATUS_FAILED, CrawlStatus.STATUS_RETRY);
      } catch (IOException | InterruptedException e) {
        LOG.error("Unexpected fetcher exception {}", e);
      } finally {
        tasksMonitor.finish(fetchTask);
      }
    } finally {
      lastTaskFinishTime.set(System.currentTimeMillis());
    }
  }

  /**
   * Multiple threaded
   * */
  public FetchResult pollFetchResut() {
    return fetchResultQueue.remove();
  }

  public boolean isFeederAlive() { return !feederThreads.isEmpty(); }

  public boolean isMissionComplete() {
    return !isFeederAlive() && tasksMonitor.readyItemCount() == 0 && tasksMonitor.pendingItemCount() == 0;
  }

  public void cleanup() {
    String border = StringUtils.repeat('.', 40);
    REPORT_LOG.info(border);
    REPORT_LOG.info("[Final Report - " + TimingUtil.now() + "]");

    feederThreads.forEach(FeederThread::exitAndJoin);
    activeFetchThreads.forEach(FetchThread::exitAndJoin);
    retiredFetchThreads.forEach(FetchThread::report);

    if (jitIndexer != null) {
      jitIndexer.cleanup();
    }

    tasksMonitor.report();
    REPORT_LOG.info("[End Report]");
    REPORT_LOG.info(border);

    LOG.info("[Final] " + context.getStatus());

    tasksMonitor.cleanup();
  }

  /**
   * Wait for a while and report task status
   *
   * @param reportInterval Report interval in milliseconds
   */
  public Status waitAndReport(long reportInterval) throws IOException {
    // Used for threshold check, holds totalPages and totalBytes processed in the last sec
    float pagesLastSec = totalPages.get();
    long bytesLastSec = totalBytes.get();

    try {
      Thread.sleep(reportInterval);
    } catch (InterruptedException ignored) {
    }

    float reportIntervalSec = reportInterval / 1000.0f;
    float pagesThrouRate = (totalPages.get() - pagesLastSec) / reportIntervalSec;
    float bytesThrouRate = (totalBytes.get() - bytesLastSec) / reportIntervalSec;

    int readyFetchItems = tasksMonitor.readyItemCount();
    int pendingFetchItems = tasksMonitor.pendingItemCount();

    counter.setValue(Counter.readyTasks, readyFetchItems);
    counter.setValue(Counter.pendingTasks, pendingFetchItems);
    counter.setValue(Counter.pagesThou, Math.round(pagesThrouRate));
    counter.setValue(Counter.mbThou, Math.round(bytesThrouRate / 1000));

    String statusString = getStatusString(pagesThrouRate, bytesThrouRate, readyFetchItems, pendingFetchItems);

    /** Status string shows in yarn admin ui */
    context.setStatus(statusString);

    /** And also log it */
    LOG.info(statusString);

    return new Status(pagesThrouRate, bytesThrouRate, readyFetchItems, pendingFetchItems);
  }

  private String getStatusString(float pagesThroughput,
                                  float bytesThroughput, int readyFetchItems, int pendingFetchItems) throws IOException {
    final DecimalFormat df = new DecimalFormat("0.0");

    this.avePageLength.set(bytesThroughput / pagesThroughput);

    StringBuilder status = new StringBuilder();
    long elapsed = (System.currentTimeMillis() - startTime) / 1000;

    status.append(idleFetchThreadCount).append("/").append(activeFetchThreadCount).append(" idle/active threads, ");
    status.append(totalPages).append(" pages, ").append(fetchErrors).append(" errors, ");

    // average speed
    status.append(df.format((((float) totalPages.get()) * 10) / 10.0 / elapsed)).append(" ");
    // instantaneous speed
    status.append(df.format((pagesThroughput * 10) / 10)).append(" pages/s, ");

    // average speed
    status.append(df.format(totalBytes.get() * 8.0 / 1024 / elapsed)).append(" ");
    // instantaneous speed
    status.append(df.format(bytesThroughput * 8 / 1024)).append(" kb/s, ");

    status.append(readyFetchItems).append(" ready ");
    status.append(pendingFetchItems).append(" pending ");
    status.append("URLs in ").append(tasksMonitor.getQueueCount()).append(" queues");

    return status.toString();
  }

  public void adjustFetchResourceByTargetBandwidth() {
    Configuration conf = getConf();

    int targetBandwidth = conf.getInt("fetcher.bandwidth.target", -1) * 1000;
    int maxNumThreads = conf.getInt("fetcher.maxNum.threads", initFetchThreadCount);
    if (maxNumThreads < initFetchThreadCount) {
      LOG.info("fetcher.maxNum.threads can't be < than " + initFetchThreadCount + " : using " + initFetchThreadCount + " instead");
      maxNumThreads = initFetchThreadCount;
    }

    int bandwidthTargetCheckEveryNSecs = conf.getInt("fetcher.bandwidth.target.check.everyNSecs", 30);
    if (bandwidthTargetCheckEveryNSecs < 1) {
      LOG.info("fetcher.bandwidth.target.check.everyNSecs can't be < to 1 : using 1 instead");
      bandwidthTargetCheckEveryNSecs = 1;
    }

    int bandwidthTargetCheckCounter = 0;
    long bytesAtLastBWTCheck = 0L;

    // adjust the number of threads if a target bandwidth has been set
    if (targetBandwidth > 0) {
      if (bandwidthTargetCheckCounter < bandwidthTargetCheckEveryNSecs) {
        bandwidthTargetCheckCounter++;
      } else if (bandwidthTargetCheckCounter == bandwidthTargetCheckEveryNSecs) {
        long bpsSinceLastCheck = ((totalBytes.get() - bytesAtLastBWTCheck) * 8) / bandwidthTargetCheckEveryNSecs;

        bytesAtLastBWTCheck = totalBytes.get();
        bandwidthTargetCheckCounter = 0;

        int averageBdwPerThread = 0;
        if (activeFetchThreadCount.get() > 0) {
          averageBdwPerThread = Math.round(bpsSinceLastCheck / activeFetchThreadCount.get());
        }

        LOG.info("averageBdwPerThread : " + (averageBdwPerThread / 1000) + " kbps");

        if (bpsSinceLastCheck < targetBandwidth && averageBdwPerThread > 0) {
          // check whether it is worth doing e.g. more queues than threads

          if ((tasksMonitor.getQueueCount() * maxThreadsPerQueue) > activeFetchThreadCount.get()) {
            long remainingBdw = targetBandwidth - bpsSinceLastCheck;
            int additionalThreads = Math.round(remainingBdw / averageBdwPerThread);
            int availableThreads = maxNumThreads - activeFetchThreadCount.get();

            // determine the number of available threads (min between
            // availableThreads and additionalThreads)
            additionalThreads = (availableThreads < additionalThreads ? availableThreads : additionalThreads);
            LOG.info("Has space for more threads ("
                + (bpsSinceLastCheck / 1000) + " vs "
                + (targetBandwidth / 1000) + " kbps) \t=> adding "
                + additionalThreads + " new threads");
            // activate new threads

            startFetchThreads(additionalThreads, conf);
          }
        } else if (bpsSinceLastCheck > targetBandwidth && averageBdwPerThread > 0) {
          // if the bandwidth we're using is greater then the expected
          // bandwidth, we have to stop some threads
          long excessBdw = bpsSinceLastCheck - targetBandwidth;
          int excessThreads = Math.round(excessBdw / averageBdwPerThread);
          LOG.info("Exceeding target bandwidth (" + bpsSinceLastCheck / 1000
              + " vs " + (targetBandwidth / 1000)
              + " kbps). \t=> excessThreads = " + excessThreads);

          // keep at least one
          if (excessThreads >= activeFetchThreads.size()) {
            excessThreads = 0;
          }

          // de-activates threads
          for (int i = 0; i < excessThreads; i++) {
            FetchThread thread = activeFetchThreads.iterator().next();
            thread.halt();
          }
        }
      } // else if (bandwidthTargetCheckCounter == bandwidthTargetCheckEveryNSecs)
    }
  }

  private void startFetchThreads(int threadCount, Configuration conf) {
    for (int i = 0; i < threadCount; i++) {
      FetchThread fetchThread = new FetchThread(this, conf);
      fetchThread.start();
    }
  }

  /**
   * Dump fetch threads
   * */
  public void dumpFetchThreads() {
    LOG.debug("There are " + activeFetchThreads.size() + " active fetch threads and " + idleFetchThreads.size() + " idle ones");

    activeFetchThreads.stream().filter(Thread::isAlive).forEach(fetchThread -> {
      StackTraceElement[] stack = fetchThread.getStackTrace();
      StringBuilder sb = new StringBuilder();
      sb.append(fetchThread.getName());
      sb.append(" -> ");
      sb.append(fetchThread.reprUrl());
      sb.append(", Stack :\n");
      for (StackTraceElement s : stack) {
        sb.append(s.toString()).append('\n');
      }

      LOG.debug(sb.toString());
    });
  }

  /**
   * Thread safe
   * */
  private void doFinishFetchTask(FetchTask fetchTask, ProtocolOutput output)
      throws IOException, InterruptedException, URLFilterException {
    final String url = fetchTask.getUrl();
    final ProtocolStatus status = output.getStatus();
    final Content content = output.getContent();

    // un-block queue
    tasksMonitor.finish(fetchTask);

    switch (status.getCode()) {
      case ProtocolStatusCodes.WOULDBLOCK:
        // retry ?
        tasksMonitor.produce(fetchTask);
        break;

      case ProtocolStatusCodes.SUCCESS:        // got a page
        handleResult(fetchTask, content, status, CrawlStatus.STATUS_FETCHED);
        tasksMonitor.statHost(url);
        break;

      case ProtocolStatusCodes.MOVED:         // redirect
      case ProtocolStatusCodes.TEMP_MOVED:
        byte code;
        boolean temp;
        if (status.getCode() == ProtocolStatusCodes.MOVED) {
          code = CrawlStatus.STATUS_REDIR_PERM;
          temp = false;
        } else {
          code = CrawlStatus.STATUS_REDIR_TEMP;
          temp = true;
        }
        // TODO : It's not a good idea to save newUrl in message
        final String newUrl = ProtocolStatusUtils.getMessage(status);
        handleRedirect(fetchTask.getUrl(), newUrl, temp, FetchJob.PROTOCOL_REDIR, fetchTask.getPage());
        handleResult(fetchTask, content, status, code);
        break;

      case ProtocolStatusCodes.CONNECTION_TIMED_OUT:
      case ProtocolStatusCodes.UNKNOWN_HOST:
        handleResult(fetchTask, null, status, CrawlStatus.STATUS_GONE);
        tasksMonitor.statUnreachableHost(url);
        break;
      case ProtocolStatusCodes.EXCEPTION:
        logFetchFailure(ProtocolStatusUtils.getMessage(status));

      /** FALL THROUGH **/
      case ProtocolStatusCodes.RETRY:          // retry
      case ProtocolStatusCodes.BLOCKED:
        handleResult(fetchTask, null, status, CrawlStatus.STATUS_RETRY);
        break;

      case ProtocolStatusCodes.GONE:           // gone
      case ProtocolStatusCodes.NOTFOUND:
      case ProtocolStatusCodes.ACCESS_DENIED:
      case ProtocolStatusCodes.ROBOTS_DENIED:
        handleResult(fetchTask, null, status, CrawlStatus.STATUS_GONE);
        break;
      case ProtocolStatusCodes.NOTMODIFIED:
        handleResult(fetchTask, null, status, CrawlStatus.STATUS_NOTMODIFIED);
        break;
      default:
        LOG.warn("Unknown ProtocolStatus : " + status.getCode());
        handleResult(fetchTask, null, status, CrawlStatus.STATUS_RETRY);
    }
  }

  private void handleRedirect(String url, String newUrl, boolean temp, String redirType, WebPage page)
      throws URLFilterException, IOException, InterruptedException {
    newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
    newUrl = urlFilters.filter(newUrl);
    if (newUrl == null || newUrl.equals(url)) {
      return;
    }

    if (ignoreExternalLinks) {
      String toHost = new URL(newUrl).getHost().toLowerCase();
      String fromHost = new URL(url).getHost().toLowerCase();
      if (!toHost.equals(fromHost)) {
        // External links
        return;
      }
    }

    page.getOutlinks().put(new Utf8(newUrl), new Utf8());
    page.getMetadata().put(FetchJob.REDIRECT_DISCOVERED, Nutch.YES_VAL);

    String reprUrl = setRedirectRepresentativeUrl(page, url, newUrl, temp);

    nutchMetrics.reportRedirects(String.format("[%s] - %100s -> %s\n", redirType, url, reprUrl), reportSuffix);

    counter.increase(Counter.rowsRedirect);
  }

  private String setRedirectRepresentativeUrl(WebPage page, String url, String newUrl, boolean temp) {
    long threadId = Thread.currentThread().getId();

    String reprUrl = reprUrls.get(threadId);
    if (reprUrl == null) {
      reprUrl = url;
    }
    reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
    if (reprUrl != null) {
      page.setReprUrl(new Utf8(reprUrl));
      reprUrls.put(threadId, reprUrl);
    }
    else {
      LOG.warn("reprUrl is null");
    }

    return reprUrl;
  }

  private void handleResult(FetchTask fetchTask, Content content, ProtocolStatus pstatus, byte status)
      throws IOException, InterruptedException {
    String url = fetchTask.getUrl();
    WebPage page = fetchTask.getPage();

    FetchUtil.setStatus(page, status, pstatus);
    FetchUtil.setContent(page, content);
    FetchUtil.setFetchTime(page);
    FetchUtil.setMarks(page);

    String reverseUrl = TableUtil.reverseUrl(url);

    // Only STATUS_FETCHED can be parsed
    // TODO : We should calculate the signature to know if a webpage is really NOTMODIFIED
    // For news, the signature must be calculated via the extracted content
    if (parse && status == CrawlStatus.STATUS_FETCHED) {
      if (!skipTruncated || !ParserMapper.isTruncated(url, page)) {
        synchronized (parseUtil) {
          parseUtil.process(reverseUrl, page);
        }

        Utf8 parseMark = Mark.PARSE_MARK.checkMark(page);
        // JIT Index
        if (jitIndexer != null && parseMark != null) {
          jitIndexer.produce(fetchTask);
        }
      }
    }

    // Debug fetch time history
    String fetchTimeHistory = TableUtil.getFetchTimeHistory(page, "");
    if (fetchTimeHistory.contains(",")) {
      String report = "Url " + fetchTask.getUrl()
          + ", status : " + CrawlStatus.getName(status)
          + ", fetchTimeHistory : " + fetchTimeHistory
          + ", signature : " + StringUtil.toHexString(page.getSignature());
      nutchMetrics.reportFetchTimeHistory(report, reportSuffix);
    }

    // Remove content if storingContent is false. Content is added to page above
    // for ParseUtil be able to parse it
    if (content != null && !storingContent) {
      page.setContent(ByteBuffer.wrap(new byte[0]));
    }

    // LOG.debug("ready to write hadoop : {}, {}", page.getStatus(), page.getMarkers());

    output(url, reverseUrl, page);

    updateStatus(fetchTask.getUrl(), page);
  }

  /**
   * Write the reduce result back to the backend storage
   *
   * threadsafe
   * */
  @SuppressWarnings("unchecked")
  private void output(String url, String reversedUrl, WebPage page) throws IOException, InterruptedException {
    if (updateJIT()) {
      Pair<UrlWithScore, WebPageWritable> updateDatum;
      List<Pair<UrlWithScore, WebPageWritable>> updateData;

      synchronized (mapDatumBuilder) {
        updateDatum = mapDatumBuilder.buildDatum(url, reversedUrl, page);
        outputWithDbUpdate(url, updateDatum.getKey(), updateDatum.getValue(), false);

        updateData = mapDatumBuilder.buildNewRows(url, page);
        updateData.stream().limit(maxDbUpdateNewRows).forEach(e -> outputWithDbUpdate(url, e.getKey(), e.getValue(), true));
      }
    }
    else {
      context.write(reversedUrl, page);
      counter.increase(Counter.rowsPeresist);
    }
  }

  @SuppressWarnings("unchecked")
  private void outputWithDbUpdate(String url, UrlWithScore urlWithScore, WebPageWritable webPageWritable, boolean isNewRow) {
    String reversedUrl = urlWithScore.getReversedUrl().toString();
    WebPage page = webPageWritable.getWebPage();

    reduceDatumBuilder.updateRowWithoutScoring(url, page);

    // LOG.debug("Write to hdfs : " + page.getBaseUrl());

    try {
      context.write(reversedUrl, page);

      if (isNewRow) {
        counter.increase(Counter.newRowsPeresist);
        if (CrawlFilter.sniffPageCategory(url) == CrawlFilter.PageCategory.DETAIL) {
          counter.increase(Counter.newDetailRows);
        }
      }
      else {
        counter.increase(Counter.rowsPeresist);
      }
    } catch (IOException|InterruptedException e) {
      LOG.error("Failed to write to hdfs" + e.toString());
    }
  }

  private void updateStatus(String url, WebPage page) throws IOException {
    int pageLength = 0;
    ByteBuffer content = page.getContent();
    if (content != null) {
      pageLength = Bytes.getBytes(page.getContent()).length;
    }

    counter.updateAffectedRows(url);
    counter.increase(Counter.mbytes, Math.round(pageLength / 1024.0f));

    totalPages.incrementAndGet();
    totalBytes.addAndGet(pageLength);
  }

  private void logFetchFailure(String message) {
    // LOG.warn("Failed to fetch " + url);

    if (!message.isEmpty()) {
      LOG.warn("Fetch failed, " + message);
    }

    // TODO : handle java.net.UnknownHostException
    // 1. we may add the host into a black list
    // 2. or we may just ignore it, because the error does not spread out in the next loop

    fetchErrors.incrementAndGet();
    counter.increase(NutchCounter.Counter.errors);
  }
}
