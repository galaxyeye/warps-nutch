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
import org.apache.nutch.crawl.UrlWithScore;
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
import org.apache.nutch.util.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nutch.metadata.Nutch.*;

public class TaskScheduler extends Configured {

  private final Logger LOG = FetchMonitor.LOG;
  private final Logger REPORT_LOG = NutchReporter.LOG;

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
    pages, bytes, errors, finishedTasks, expiredQueues, unknowHosts, unexpectedErrors,
    readyTasks, pendingTasks,
    pagesThroughput, bytesThroughput,
    waitingFetchThreads, activeFetchThreads,
    rowsWritten, newRowsWritten
  }

  public static final int QUEUE_REMAINDER_LIMIT = 5;

  private static AtomicInteger objectSequence = new AtomicInteger(0);

  private final int id;

  private final NutchContext context;
  private NutchCounter counter;

  private TasksMonitor tasksMonitor;// all fetch items are contained in several queues
  private Queue<FetchResult> fetchResultQueue = new ConcurrentLinkedQueue<>();

  /**
   * Our own Hardware bandwidth in Mbytes, if exceed the limit, slows down the task scheduling.
   * TODO : automatically adjust
   */
  private final int bandwidth;

  // handle redirect
  private final URLFilters urlFilters;
  private final URLNormalizers normalizers;
  private final boolean ignoreExternalLinks;

  // parser setting
  private final boolean storingContent;
  private final boolean parse;
  private final ParseUtil parseUtil;
  private final boolean skipTruncated;

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

  /** Indexer */
  private JITIndexer JITIndexer;

  /** Update */
  private boolean updateJIT;
  private MapDatumBuilder mapDatumBuilder;
  private ReduceDatumBuilder reduceDatumBuilder;

  // Timer
  private final long startTime = System.currentTimeMillis(); // start time of fetcher run
  private final AtomicLong lastTaskStartTime = new AtomicLong(startTime);
  private final AtomicLong lastTaskFinishTime = new AtomicLong(startTime);

  // statistics
  private final AtomicLong totalBytes = new AtomicLong(0);        // total fetched bytes
  private final AtomicInteger totalPages = new AtomicInteger(0);  // total fetched pages
  private final AtomicInteger fetchErrors = new AtomicInteger(0); // total fetch fetchErrors

  private final AtomicDouble avePageLength = new AtomicDouble(0.0);

  private String reprUrl; // choosed representative url

  /** Output */
  private Path outputDir;

  @SuppressWarnings("rawtypes")
  public TaskScheduler(NutchCounter counter, NutchContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    setConf(conf);

    this.id = objectSequence.incrementAndGet();
    this.counter = counter;
    this.context = context;

    this.bandwidth = 1024 * 1024 * conf.getInt("fetcher.net.bandwidth.m", Integer.MAX_VALUE);
    this.initFetchThreadCount = conf.getInt("fetcher.threads.fetch", 10);
    this.maxThreadsPerQueue = conf.getInt("fetcher.threads.per.queue", 1);
    this.maxFeedPerThread = conf.getInt("fetcher.queue.depth.multiplier", 100);

    this.urlFilters = new URLFilters(conf);
    this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);

    this.tasksMonitor = new TasksMonitor(conf);

    // index manager
    boolean indexJIT = conf.getBoolean(Nutch.PARAM_INDEX_JUST_IN_TIME, false);
    this.JITIndexer = indexJIT ? new JITIndexer(conf) : null;

    this.updateJIT = getConf().getBoolean(PARAM_DBUPDATE_JUST_IN_TIME, true);
    if (updateJIT) {
      mapDatumBuilder = new MapDatumBuilder(counter, conf);
      reduceDatumBuilder = new ReduceDatumBuilder(counter, conf);
    }

    this.parse = indexJIT || conf.getBoolean(PARAM_PARSE, false);
    this.parseUtil = parse ? new ParseUtil(getConf()) : null;
    this.skipTruncated = getConf().getBoolean(ParserJob.SKIP_TRUNCATED, true);
    this.ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    this.storingContent = conf.getBoolean("fetcher.store.content", true);

    this.outputDir = NutchConfiguration.getPath(conf, PARAM_NUTCH_OUTPUT_DIR, Paths.get(PATH_NUTCH_OUTPUT_DIR));

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

        "indexJIT", indexJIT,
        "outputDir", outputDir
    ));
  }

  public int getId() { return id; }

  public String name() { return getClass().getSimpleName() + "-" + id; }

  public int getBandwidth() { return this.bandwidth; }

  public TasksMonitor getTasksMonitor() {
    return tasksMonitor;
  }

  public JITIndexer getJITIndexer() {
    return JITIndexer;
  }

  public long getLastTaskFinishTime() {
    return lastTaskFinishTime.get();
  }

  public int getActiveFetchThreadCount() { return activeFetchThreadCount.get(); }

  public int getFeedLimit() {
    return initFetchThreadCount * maxFeedPerThread;
  }

  public boolean indexJIT() { return JITIndexer != null; }

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
      LOG.error("Failed to finish task [{} - {}]", queueID, itemID);

      return;
    }

    try {
      doFinishFetchTask(fetchTask, output);
    } catch (final Throwable t) {
      LOG.error("Unexpected error for " + fetchTask.getUrl(), t);

      tasksMonitor.finish(fetchTask);
      fetchErrors.incrementAndGet();

      try {
        output(fetchTask, null, ProtocolStatusUtils.STATUS_FAILED, CrawlStatus.STATUS_RETRY);
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
    return !isFeederAlive()
        && tasksMonitor.readyItemCount() == 0
        && tasksMonitor.pendingItemCount() == 0;
  }

  public void cleanup() {
    String outerBorder = StringUtils.repeat('-', 100);
    String innerBorder = StringUtils.repeat('.', 100);
    REPORT_LOG.info(outerBorder);
    REPORT_LOG.info(innerBorder);
    REPORT_LOG.info("[Final Report]");

    feederThreads.forEach(FeederThread::halt);
    waitUtilEmpty(feederThreads, 10 * 1000);

    activeFetchThreads.forEach(FetchThread::halt);
    waitUtilEmpty(activeFetchThreads, 10 * 1000);

    retiredFetchThreads.forEach(FetchThread::report);

    if (JITIndexer != null) {
      JITIndexer.cleanup();
    }

    REPORT_LOG.info("[Final]" + context.getStatus());
    tasksMonitor.report();
    REPORT_LOG.info("[End Report]");
    REPORT_LOG.info(innerBorder);
    REPORT_LOG.info(outerBorder);

    tasksMonitor.cleanup();
  }

  /**
   * Collection must be thread safe
   * */
  private void waitUtilEmpty(Collection collection, long timeout) {
    try {
      while (!collection.isEmpty()) {
        Thread.sleep(1000);
      }
    } catch (final Exception ignored) {}
  }

  /**
   * Wait for a while and report task status
   *
   * @param reportInterval Report interval
   */
  public Status waitAndReport(int reportInterval) throws IOException {
    // Used for threshold check, holds totalPages and totalBytes processed in the last sec
    float pagesLastSec = totalPages.get();
    long bytesLastSec = totalBytes.get();

    try {
      Thread.sleep(reportInterval * 1000);
    } catch (InterruptedException ignored) {
    }

    float pagesThroughputRate = (totalPages.get() - pagesLastSec) / reportInterval;
    float bytesThroughputRate = (totalBytes.get() - bytesLastSec) / reportInterval;

    int readyFetchItems = tasksMonitor.readyItemCount();
    int pendingFetchItems = tasksMonitor.pendingItemCount();

    counter.setValue(Counter.activeFetchThreads, activeFetchThreadCount.get());
    counter.setValue(Counter.waitingFetchThreads, idleFetchThreadCount.get());
    counter.setValue(Counter.readyTasks, readyFetchItems);
    counter.setValue(Counter.pendingTasks, pendingFetchItems);
    counter.setValue(Counter.pagesThroughput, Math.round(pagesThroughputRate));
    counter.setValue(Counter.bytesThroughput, Math.round(bytesThroughputRate));

    String statusString = getStatusString(pagesThroughputRate, bytesThroughputRate, readyFetchItems, pendingFetchItems);

    /** status string shows in yarn admin ui */
    context.setStatus(statusString);
    LOG.info(statusString);

    return new Status(pagesThroughputRate, bytesThroughputRate, readyFetchItems, pendingFetchItems);
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

  private void doFinishFetchTask(FetchTask fetchTask, ProtocolOutput output) throws IOException, InterruptedException, URLFilterException {
    final ProtocolStatus status = output.getStatus();
    final Content content = output.getContent();

    // unblock queue
    tasksMonitor.finish(fetchTask);

    switch (status.getCode()) {
      case ProtocolStatusCodes.WOULDBLOCK:
        // retry ?
        tasksMonitor.produce(fetchTask);
        break;

      case ProtocolStatusCodes.SUCCESS:        // got a page
        output(fetchTask, content, status, CrawlStatus.STATUS_FETCHED);
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
        final String newUrl = ProtocolStatusUtils.getMessage(status);
        handleRedirect(fetchTask.getUrl(), newUrl, temp, FetchJob.PROTOCOL_REDIR, fetchTask.getPage());
        output(fetchTask, content, status, code);
        break;

      case ProtocolStatusCodes.CONNECTION_TIMED_OUT:
      case ProtocolStatusCodes.UNKNOWN_HOST:
        String domain = URLUtil.getDomainName(fetchTask.getUrl());
        tasksMonitor.addUnreachableHost(domain);
        output(fetchTask, null, status, CrawlStatus.STATUS_GONE);
        break;
      case ProtocolStatusCodes.EXCEPTION:
        logFetchFailure(ProtocolStatusUtils.getMessage(status));

      /* FALLTHROUGH */
      case ProtocolStatusCodes.RETRY:          // retry
      case ProtocolStatusCodes.BLOCKED:
        output(fetchTask, null, status, CrawlStatus.STATUS_RETRY);
        break;

      case ProtocolStatusCodes.GONE:           // gone
      case ProtocolStatusCodes.NOTFOUND:
      case ProtocolStatusCodes.ACCESS_DENIED:
      case ProtocolStatusCodes.ROBOTS_DENIED:
        output(fetchTask, null, status, CrawlStatus.STATUS_GONE);
        break;
      case ProtocolStatusCodes.NOTMODIFIED:
        output(fetchTask, null, status, CrawlStatus.STATUS_NOTMODIFIED);
        break;
      default:
        LOG.warn("Unknown ProtocolStatus: " + status.getCode());
        output(fetchTask, null, status, CrawlStatus.STATUS_RETRY);
    } // switch
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
        // external links
        return;
      }
    }

    page.getOutlinks().put(new Utf8(newUrl), new Utf8());
    page.getMetadata().put(FetchJob.REDIRECT_DISCOVERED, Nutch.YES_VAL);
    reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
    if (reprUrl == null) {
      LOG.warn("reprUrl==null");
    } else {
      page.setReprUrl(new Utf8(reprUrl));
      if (LOG.isDebugEnabled()) {
        LOG.debug("[" + redirType + "] redirect to " + reprUrl + " , fetching later");
      }
    }
  }

  /**
   * Write the reduce result back to the backend storage
   *
   * Multiple threaded
   * */
  @SuppressWarnings("unchecked")
  private void output(FetchTask fetchTask, Content content, ProtocolStatus pstatus, byte status)
      throws IOException, InterruptedException {
    String url = fetchTask.getUrl();
    WebPage page = fetchTask.getPage();

    FetchUtil.setStatus(page, status, pstatus);
    FetchUtil.setContent(page, content);
    FetchUtil.setFetchTime(page);
    FetchUtil.setMarks(page);

    String key = TableUtil.reverseUrl(url);

    // Only STATUS_FETCHED can be parsed
    // TODO : We should calculate the signature to know if a webpage is really NOTMODIFIED
    // For news, the signature must be calculated via the extracted content
    if (parse && status == CrawlStatus.STATUS_FETCHED) {
      if (!skipTruncated || !ParserMapper.isTruncated(url, page)) {
        synchronized (parseUtil) {
          parseUtil.process(key, page);
        }

        Utf8 parseMark = Mark.PARSE_MARK.checkMark(page);
        // JIT Index
        if (JITIndexer != null && parseMark != null) {
          JITIndexer.produce(fetchTask);
        }
      }
    }

    String fetchTimeHistory = TableUtil.getFetchTimeHistory(page, "");
    if (fetchTimeHistory.contains(",")) {
      LOG.info("Url " + fetchTask.getUrl()
          + ", status : " + CrawlStatus.getName(status)
          + ", fetchTimeHistory : " + fetchTimeHistory
          + ", signature : " + StringUtil.toHexString(page.getSignature())
      );
    }

    // Remove content if storingContent is false. Content is added to page above
    // for ParseUtil be able to parse it
    if (content != null && !storingContent) {
      page.setContent(ByteBuffer.wrap(new byte[0]));
    }

    // LOG.debug("ready to write hadoop : {}, {}", page.getStatus(), page.getMarkers());

    if (updateJIT) {
      Pair<UrlWithScore, WebPageWritable> updateDatum = mapDatumBuilder.buildDatum(url, key, page);
      output(updateDatum.getKey(), updateDatum.getValue());
      counter.increase(Counter.rowsWritten);

      List<Pair<UrlWithScore, WebPageWritable>> updateData = mapDatumBuilder.buildNewRows(url, page);
      updateData.forEach(e -> output(e.getKey(), e.getValue()));
      counter.increase(Counter.newRowsWritten, updateData.size());
    }
    else {
      context.write(key, page);
      counter.increase(Counter.rowsWritten);
    }

    updateStatus(fetchTask.getUrl(), page);
  }

  private void output(UrlWithScore urlWithScore, WebPageWritable webPageWritable) {
    String key = urlWithScore.getReversedUrl().toString();
    WebPage page = webPageWritable.getWebPage();

    reduceDatumBuilder.updateRow(key, page);

    // LOG.debug("Write to hdfs : " + page.getBaseUrl());

    try {
      context.write(key, page);
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
    counter.increase(Counter.pages);
    counter.increase(Counter.bytes, pageLength);

    totalPages.incrementAndGet();
    totalBytes.addAndGet(pageLength);
  }

  private void logFetchFailure(String message) {
    // LOG.warn("Failed to fetch " + url);

    if (!message.isEmpty()) {
      LOG.warn(message);
    }

    // TODO : handle java.net.UnknownHostException
    // 1. we may add the host into a black list
    // 2. or we may just ignore it, because the error does not spread out in the next loop

    fetchErrors.incrementAndGet();
    counter.increase(Counter.errors);
  }
}
