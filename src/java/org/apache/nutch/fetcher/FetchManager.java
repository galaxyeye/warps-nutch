package org.apache.nutch.fetcher;

import com.google.common.collect.Lists;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.fetcher.data.FetchItem;
import org.apache.nutch.fetcher.data.FetchItemQueues;
import org.apache.nutch.fetcher.data.FetchResult;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.parse.ParserMapper;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class FetchManager extends Configured {

  static private final Logger LOG = FetcherJob.LOG;

  enum Counter {
    pages, bytes, errors, finishedTasks, expiredQueues, unexpectedErrors,
    readyFetchItems, pendingFetchItems,
    pagesThroughput, bytesThroughput,
    waitingFetchThreadCount, activeFetchThreadCount
  };

  private Integer jobID;

  @SuppressWarnings("rawtypes")
  private final Context context;

  private FetchItemQueues fetchItemQueues;// all fetch items are contained in several queues
  private List<FetchResult> fetchResultQueue = Collections.synchronizedList(new LinkedList<FetchResult>());

  /**
   * Our own Hardware bandwidth in Mbytes, if exceed the limit, slows down the task scheduling.
   * TODO : automatically adjust
   * */
  private final int bandwidth;

  // handle redirect
  private URLFilters urlFilters;
  private URLNormalizers normalizers;
  private String reprUrl; // choosed representative url
  private boolean ignoreExternalLinks;

  // parser setting
  private final boolean storingContent;
  private boolean parse;
  private ParseUtil parseUtil;
  private boolean skipTruncated;

  private float avePageLength = 0;

  private final int fetchThreadCount;
  private final int maxThreadsPerQueue;

  private final LinkedList<FetchThread> fetchThreads = Lists.newLinkedList();
  private final AtomicInteger activeFetchThreadCount = new AtomicInteger(0);
  private final AtomicInteger waitingFetchThreadCount = new AtomicInteger(0);

  // timer
  private final long startTime = System.currentTimeMillis(); // start time of fetcher run
  private final AtomicLong lastTaskStartTime = new AtomicLong(startTime);
  private final AtomicLong lastTaskFinishTime = new AtomicLong(startTime);

  // statistics
  private final AtomicLong totalBytes = new AtomicLong(0);        // total fetched bytes
  private final AtomicInteger totalPages = new AtomicInteger(0);  // total fetched pages
  private final AtomicInteger fetchErrors = new AtomicInteger(0); // total fetch fetchErrors
  /**
   * fetch speed counted by totalPages per second
   * */
  private float pagesThroughputRate = 0;
  /**
   * fetch speed counted by total totalBytes per second
   * */
  private float bytesThroughputRate = 0;

  private NutchCounter counter;

  @SuppressWarnings("rawtypes")
  FetchManager(int jobID, NutchCounter counter, Context context) {
    Configuration conf = context.getConfiguration();
    setConf(conf);

    this.jobID = jobID;
    this.context = context;
    this.counter = counter;

    this.bandwidth = 1024 * 1024 * conf.getInt("fetcher.net.bandwidth.m", Integer.MAX_VALUE);
    this.fetchThreadCount = conf.getInt("fetcher.threads.fetch", 10);
    this.maxThreadsPerQueue = conf.getInt("fetcher.threads.per.queue", 1);

    this.urlFilters = new URLFilters(conf);
    this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
    this.ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);

    this.storingContent = conf.getBoolean("fetcher.store.content", true);

    this.parse = conf.getBoolean(FetcherJob.PARSE_KEY, false);
    if (parse) {
      this.skipTruncated = getConf().getBoolean(ParserJob.SKIP_TRUNCATED, true);
      this.parseUtil = new ParseUtil(getConf());
    }

    try {
      this.fetchItemQueues = new FetchItemQueues(conf);
    } catch (IOException e) {
      LOG.error(e.toString());
    }

    LOG.info(StringUtil.formatParams(
        "jobID", jobID,
        "bandwidth", bandwidth,
        "storingContent", storingContent,
        "ignoreExternalLinks", ignoreExternalLinks
    ));
  }

  String name() { return "FetchManager-" + jobID; }

  Integer jobID() {
    return jobID;
  }

  int getBandwidth() {
    return this.bandwidth;
  }

  int getQueueCount() {
    return fetchItemQueues.getQueueCount();
  }

  int getReadyItemCount() {
    return fetchItemQueues.getReadyItemCount();
  }

  int getPendingItemCount() {
    return fetchItemQueues.getPendingItemCount();
  }

  FetchItemQueues getFetchItemQueues() {
    return fetchItemQueues;
  }

  long getLastTaskStartTime() {
    return lastTaskStartTime.get();
  }

  long getLastTaskFinishTime() {
    return lastTaskFinishTime.get();
  }

  /**
   * Instantaneous speed
   * @return instantaneous speed
   * */
  float getPagesThroughputRate() { return pagesThroughputRate; }

  float getBytesThroughput() { return bytesThroughputRate; }

  int getActiveFetchThreadCount() { return activeFetchThreadCount.get(); }

  void setParseOnFetch(boolean parse) {
    this.parse = parse;
    if (parse) {
      this.skipTruncated = getConf().getBoolean(ParserJob.SKIP_TRUNCATED, true);
      this.parseUtil = new ParseUtil(getConf());
    }
  }

  int clearFetchItemQueues() {
    return fetchItemQueues.clearQueues();
  }

  void startFetchThreads(QueueFeederThread queueFeederThread, int threadCount, Context context) {
    for (int i = 0; i < threadCount; i++) {
      FetchThread fetchThread = new FetchThread(queueFeederThread, this, context);
      fetchThreads.add(fetchThread);
      fetchThread.start();
    }
  }

  /**
   * consume a fetch item and try to download the target web page
   * 
   * TODO : add FetchQueues.consumeFetchItems to make locking inside loop
   * 
   * 
   * */
  List<FetchItem> consumeFetchItems(int number) {
    List<FetchItem> fetchItems = Lists.newArrayList();
    if (number <= 0) {
      return fetchItems;
    }

    // bandwidth exhausted, slows down scheduling
    if (getPendingItemCount() * avePageLength * 8 > 30 * this.getBandwidth()) {
      return fetchItems;
    }

    // TODO : avoid hard coding
    if (getPendingItemCount() > 30) {
      return fetchItems;
    }

    while (number-- > 0) {
      FetchItem fetchItem = fetchItemQueues.consumeFetchItem();
      if (fetchItem != null) fetchItems.add(fetchItem);
    }

    if (!fetchItems.isEmpty()) {
      lastTaskStartTime.set(System.currentTimeMillis());
    }

    return fetchItems;
  }

  FetchItem consumeFetchItem() {
    List<FetchItem> fetchItems = consumeFetchItems(1);
    return fetchItems.isEmpty() ? null : fetchItems.iterator().next();
  }

  void produceFetchResut(FetchResult result) {
    fetchResultQueue.add(result);
  }

  FetchResult consumeFetchResut() {
    try {
      return fetchResultQueue.remove(0);
    }
    catch(IndexOutOfBoundsException e) {
    }

    return null;
  }

  FetchItem getPendingFetchItem(String queueID, long itemID) {
    return fetchItemQueues.getPendingFetchItem(queueID, itemID);
  }

  /**
   * Finish the fetch item anyway, even if it's failed to download the target page
   * */
  void finishFetchItem(FetchItem fetchItem) {
    fetchItemQueues.finishFetchItem(fetchItem);
    lastTaskFinishTime.set(System.currentTimeMillis());
  }

  /**
   * Finished downloading the web page
   * */
  void finishFetchItem(String queueID, long itemID, ProtocolOutput output) {
    FetchItem fetchItem = fetchItemQueues.getPendingFetchItem(queueID, itemID);

    if (fetchItem == null) {
      LOG.error("Bad fetch item {} - {}", queueID, itemID);

      return;
    }

    try {
//      LOG.info("finished " + fetchItem.getUrl() + " (queue crawl delay=" + 
//                fetchItemQueues.getFetchItemQueue(fetchItem.getQueueID()).getCrawlDelay() + "ms)");

      doFinishFetchTask(fetchItem, output);

      counter.increase(Counter.finishedTasks);
    }
    catch (final Throwable t) {
      // unexpected exception
      // unblock
      fetchItemQueues.finishFetchItem(fetchItem);
      LOG.error("Unexpected error for " + fetchItem.getUrl(), t);
      counter.increase(Counter.unexpectedErrors);

      try {
        output(fetchItem, null, ProtocolStatusUtils.STATUS_FAILED, CrawlStatus.STATUS_RETRY);
      }
      catch (IOException | InterruptedException e) {
        LOG.error("fetcher throwable caught", e);
      }
      finally {
        if (fetchItem != null) {
          fetchItemQueues.finishFetchItem(fetchItem);
        }
      }
    }
    finally {
      lastTaskFinishTime.set(System.currentTimeMillis());
    }
  }

  void reviewPendingFetchItems(boolean force) {
    fetchItemQueues.reviewPendingFetchItems(force);
  }

  /**
   * Wait for a while and report task status
   * @param reportInterval Report interval
   * @param context
   * */
  void waitAndReport(int reportInterval, Context context) throws IOException {
    // Used for threshold check, holds totalPages and totalBytes processed in the last sec
    float pagesLastSec = totalPages.get();
    long bytesLastSec = totalBytes.get();

    try {
      Thread.sleep(reportInterval * 1000);
    } catch (InterruptedException ignored) {}

    pagesThroughputRate = (totalPages.get() - pagesLastSec) / reportInterval;
    bytesThroughputRate = (totalBytes.get() - bytesLastSec) / reportInterval;

    int readyFetchItems = getReadyItemCount();
    int pendingFetchItems = getPendingItemCount();

    counter.setValue(Counter.activeFetchThreadCount, activeFetchThreadCount.get());
    counter.setValue(Counter.waitingFetchThreadCount, waitingFetchThreadCount.get());
    counter.setValue(Counter.readyFetchItems, readyFetchItems);
    counter.setValue(Counter.pendingFetchItems, pendingFetchItems);
    counter.setValue(Counter.pagesThroughput, Math.round(pagesThroughputRate));
    counter.setValue(Counter.bytesThroughput, Math.round(bytesThroughputRate));

    reportAndLogStatus(context, pagesThroughputRate, bytesThroughputRate, readyFetchItems, pendingFetchItems);
  }

  private void reportAndLogStatus(Context context, float pagesThroughput,
                                  float bytesThroughput, int readyFetchItems, int pendingFetchItems) throws IOException {

    this.avePageLength = bytesThroughput / pagesThroughput;

    StringBuilder status = new StringBuilder();
    long elapsed = (System.currentTimeMillis() - startTime) / 1000;

    status.append(waitingFetchThreadCount).append("/").append(activeFetchThreadCount).append(" waiting/active threads, ");
    status.append(totalPages).append(" pages, ").append(fetchErrors).append(" errors, ");

    // average speed
    status.append(Math.round((((float) totalPages.get())*10)/elapsed)/10.0).append(" ");
    // instantaneous speed
    status.append(Math.round((pagesThroughput*10)/10.0)).append(" pages/s, ");

    // average speed
    status.append(Math.round((((float) totalBytes.get())*8)/1024)/elapsed).append(" ");
    // instantaneous speed
    status.append(Math.round((bytesThroughput)*8)/1024).append(" kb/s, ");

    status.append(readyFetchItems).append(" ready ");
    status.append(pendingFetchItems).append(" pending ");
    status.append("URLs in ").append(getQueueCount()).append(" queues");

    String statusString = status.toString();
    /** status string shows in yarn admin ui */
    context.setStatus(statusString);
    LOG.info(statusString);
  }

  void adjustFetchResourceByTargetBandwidth(QueueFeederThread queueFeederThread) {
    Configuration conf = getConf();

    int targetBandwidth = conf.getInt("fetcher.bandwidth.target", -1) * 1000;
    int maxNumThreads = conf.getInt("fetcher.maxNum.threads", fetchThreadCount);
    if (maxNumThreads < fetchThreadCount) {
      LOG.info("fetcher.maxNum.threads can't be < than " + fetchThreadCount + " : using " + fetchThreadCount + " instead");
      maxNumThreads = fetchThreadCount;
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
      }
      else if (bandwidthTargetCheckCounter == bandwidthTargetCheckEveryNSecs) {
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

          if ((getQueueCount() * maxThreadsPerQueue) > activeFetchThreadCount.get()) {
            long remainingBdw = targetBandwidth - bpsSinceLastCheck;
            int additionalThreads = Math.round(remainingBdw / averageBdwPerThread);
            int availableThreads = maxNumThreads - activeFetchThreadCount.get();

            // determine the number of available threads (min between
            // availableThreads and additionalThreads)
            additionalThreads = (availableThreads < additionalThreads ? availableThreads
                : additionalThreads);
            LOG.info("Has space for more threads ("
                + (bpsSinceLastCheck / 1000) + " vs "
                + (targetBandwidth / 1000) + " kbps) \t=> adding "
                + additionalThreads + " new threads");
            // activate new threads

            startFetchThreads(queueFeederThread, additionalThreads, context);
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
          if (excessThreads >= fetchThreads.size()) {
            excessThreads = 0;            
          }
          
          // de-activates threads
          for (int i = 0; i < excessThreads; i++) {
            FetchThread thread = fetchThreads.removeLast();
            thread.setHalted(true);
          }
        }
      } // else if (bandwidthTargetCheckCounter == bandwidthTargetCheckEveryNSecs)
    }
  }

  /**
   * Dump fetch items
   * */
  synchronized void dumpFetchItems(int limit) {
    fetchItemQueues.dump(limit);
  }

  /**
   * Dump fetch threads
   * */
  synchronized void dumpFetchThreads() {
    for (int i = 0; i < fetchThreads.size(); i++) {
      FetchThread thread = fetchThreads.get(i);
      if (thread.isAlive()) {
        LOG.warn("Thread #" + i + " hung while processing " + thread.reprUrl());

        // TODO : how to report to the web panel?

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
  }

  private void doFinishFetchTask(FetchItem fetchItem, ProtocolOutput output) throws IOException, InterruptedException, URLFilterException {
    final ProtocolStatus status = output.getStatus();
    final Content content = output.getContent();

    // unblock queue
    fetchItemQueues.finishFetchItem(fetchItem);

    int length = 0;
    if (content != null && content.getContent() != null) {
      length = content.getContent().length;
    }

    updateStatus(fetchItem.getUrl(), length);

    switch(status.getCode()) {
    case ProtocolStatusCodes.WOULDBLOCK:
      // retry ?
      fetchItemQueues.produceFetchItem(fetchItem);
      break;

    case ProtocolStatusCodes.SUCCESS:        // got a page
      output(fetchItem, content, status, CrawlStatus.STATUS_FETCHED);
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
      handleRedirect(fetchItem.getUrl(), newUrl, temp,  FetcherJob.PROTOCOL_REDIR, fetchItem.getPage());
      output(fetchItem, content, status, code);
      break;
    case ProtocolStatusCodes.EXCEPTION:
      logFetchFailure(fetchItem.getUrl(), ProtocolStatusUtils.getMessage(status));
      /* FALLTHROUGH */
    case ProtocolStatusCodes.RETRY:          // retry
    case ProtocolStatusCodes.BLOCKED:
      output(fetchItem, null, status, CrawlStatus.STATUS_RETRY);
      break;

    case ProtocolStatusCodes.GONE:           // gone
    case ProtocolStatusCodes.NOTFOUND:
    case ProtocolStatusCodes.ACCESS_DENIED:
    case ProtocolStatusCodes.ROBOTS_DENIED:
      output(fetchItem, null, status, CrawlStatus.STATUS_GONE);
      break;
    case ProtocolStatusCodes.NOTMODIFIED:
      output(fetchItem, null, status, CrawlStatus.STATUS_NOTMODIFIED);
      break;
    default:
      if (LOG.isWarnEnabled()) {
        LOG.warn("Unknown ProtocolStatus: " + status.getCode());
      }

      output(fetchItem, null, status, CrawlStatus.STATUS_RETRY);
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
      String toHost   = new URL(newUrl).getHost().toLowerCase();
      String fromHost = new URL(url).getHost().toLowerCase();
      if (toHost == null || !toHost.equals(fromHost)) {
        // external links
        return;
      }
    }

    page.getOutlinks().put(new Utf8(newUrl), new Utf8());
    page.getMetadata().put(FetcherJob.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
    reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
    if (reprUrl == null) {
      LOG.warn("reprUrl==null");
    } else {
      page.setReprUrl(new Utf8(reprUrl));
      if (LOG.isDebugEnabled()) {
        LOG.debug(" - " + redirType + " redirect to " + reprUrl + " (fetching later)");
      }
    }
  }

  /**
   * Write the reduce result back to the backend storage
   * */
  @SuppressWarnings("unchecked")
  private void output(FetchItem fetchItem, Content content, ProtocolStatus pstatus, byte status)
  throws IOException, InterruptedException {
    String url = fetchItem.getUrl();
    WebPage page = fetchItem.getPage();

    page.setStatus((int)status);
    final long prevFetchTime = page.getFetchTime();
    page.setPrevFetchTime(prevFetchTime);
    page.setFetchTime(System.currentTimeMillis());
    if (pstatus != null) {
      page.setProtocolStatus(pstatus);
    }

    if (content != null) {
      page.setContent(ByteBuffer.wrap(content.getContent()));
      page.setContentType(new Utf8(content.getContentType()));
      page.setBaseUrl(new Utf8(content.getBaseUrl()));
    }

    Mark.FETCH_MARK.putMark(page, Mark.GENERATE_MARK.checkMark(page));
    String key = TableUtil.reverseUrl(url);

    if (parse) {
      if (!skipTruncated || !ParserMapper.isTruncated(url, page)) {
        parseUtil.process(key, page);
      }
    }

    // Remove content if storingContent is false. Content is added to page above
    // for ParseUtil be able to parse it.
    if(content != null && !storingContent) {
      page.setContent(ByteBuffer.wrap(new byte[0]));
    }

    // LOG.debug("ready to write hadoop : {}, {}", page.getStatus(), page.getMarkers());

    context.write(key, page);
  }

  private void updateStatus(String url, int bytesInPage) throws IOException {
    counter.updateAffectedRows(url);
    counter.increase(Counter.pages);
    counter.increase(Counter.bytes, bytesInPage);

    totalPages.incrementAndGet();
    totalBytes.addAndGet(bytesInPage);
  }

  private void logFetchFailure(String url, String message) {
    LOG.warn("Failed to fetch " + url);
    if (!message.isEmpty()) {
      LOG.warn(message);
    }
    fetchErrors.incrementAndGet();
    counter.increase(Counter.errors);
  }
}
