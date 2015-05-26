package org.apache.nutch.fetcher;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.fetcher.data.FetchItem;
import org.apache.nutch.fetcher.data.FetchItemQueues;
import org.apache.nutch.fetcher.data.FetchResult;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.NutchUtil;
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
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

/**
 * TODO : check thread safe
 * */
public class FetchManager extends Configured {

  public static final Logger LOG = FetcherJob.LOG;

  public static enum Counter {
    pages, bytes, errors, finishedTasks, expiredQueues, unexpectedErrors, readyFetchItems, pendingFetchItems,
    waitingFetcherThreads, activeFetcherThreads
  };

  private Integer jobID;

  @SuppressWarnings("rawtypes")
  private final Context context;

  private FetchItemQueues fetchItemQueues;// all fetch items are contained in several queues
  private List<FetchResult> fetchResultQueue = Collections.synchronizedList(new LinkedList<FetchResult>());

  /**
   * Hardware bandwidth in Mbytes, if exceed the limit, slows down the task scheduling.
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

  // statistics
  private final AtomicLong bytes = new AtomicLong(0);        // total bytes fetched
  private final AtomicInteger pages = new AtomicInteger(0);  // total pages fetched
  private final AtomicInteger errors = new AtomicInteger(0); // total pages errored

  private float avePageLength = 0;

  // TODO : make them to be private
  final AtomicInteger activeFetcherThreads = new AtomicInteger(0);
  final AtomicInteger waitingFetcherThreads = new AtomicInteger(0);

  private final long startTime = System.currentTimeMillis(); // start time of fetcher run
  private final AtomicLong lastTaskStartTime = new AtomicLong(startTime);
  private final AtomicLong lastTaskFinishTime = new AtomicLong(startTime);

  private long timeLimitMillis = -1;

  private NutchCounter counter;

  @SuppressWarnings("rawtypes")
  public FetchManager(int jobID, NutchCounter counter, Context context) {
    Configuration conf = context.getConfiguration();
    setConf(conf);

    this.jobID = jobID;
    this.context = context;
    this.counter = counter;

    this.bandwidth = 1024 * 1024 * conf.getInt("fetcher.net.bandwidth.m", Integer.MAX_VALUE);

    this.urlFilters = new URLFilters(conf);
    this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
    this.ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);

    this.storingContent = conf.getBoolean("fetcher.store.content", true);
    this.timeLimitMillis = 1000 * 60 * conf.getLong("fetcher.timeLimitMillis.mins", -1);

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

    LOG.info(NutchUtil.printArgMap(
        "jobID", jobID,
        "bandwidth", bandwidth,
        "timeLimitMillis", timeLimitMillis,
        "storingContent", storingContent,
        "ignoreExternalLinks", ignoreExternalLinks
    ));
  }

  public Integer jobID() {
    return jobID;
  }

  public int getBandwidth() {
    return this.bandwidth;
  }

  public int getQueueCount() {
    return fetchItemQueues.getQueueCount();
  }

  public int getReadyItemCount() {
    return fetchItemQueues.getReadyItemCount();
  }

  public int getPendingItemCount() {
    return fetchItemQueues.getPendingItemCount();
  }

  public FetchItemQueues getFetchItemQueues() {
    return fetchItemQueues;
  }

  public long getLastTaskStartTime() {
    return this.lastTaskStartTime.get();
  }

  public long getLastTaskFinishTime() {
    return this.lastTaskFinishTime.get();
  }

  public void setParseOnFetch(boolean parse) {
    this.parse = parse;
    if (parse) {
      this.skipTruncated = getConf().getBoolean(ParserJob.SKIP_TRUNCATED, true);
      this.parseUtil = new ParseUtil(getConf());
    }
  }

  public int clearFetchItemQueues() {
    return fetchItemQueues.clearQueues();
  }

  /**
   * TODO : check the real meaning of timeLimitMillis
   * 
   * It seems to be "fetch deadline"
   * */
  public int checkTimelimit() {
    if (timeLimitMillis > 0 && System.currentTimeMillis() >= timeLimitMillis) {
      return clearFetchItemQueues();
    }

    return 0;
  }

  public void dumpFetchItemQueues(int limit) {
    fetchItemQueues.dump(limit);
  }

  /**
   * consume a fetch item and try to download the target web page
   * 
   * TODO : add FetchQueues.consumeFetchItems to make locking inside loop
   * 
   * 
   * */
  public List<FetchItem> consumeFetchItems(int number) {
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

  public FetchItem consumeFetchItem() {
    List<FetchItem> fetchItems = consumeFetchItems(1);
    return fetchItems.isEmpty() ? null : fetchItems.iterator().next();
  }

  public void produceFetchResut(FetchResult result) {
    fetchResultQueue.add(result);
  }

  public FetchResult consumeFetchResut() {
    try {
      return fetchResultQueue.remove(0);
    }
    catch(IndexOutOfBoundsException e) {
    }

    return null;
  }

  public FetchItem getPendingFetchItem(String queueID, long itemID) {
    return fetchItemQueues.getPendingFetchItem(queueID, itemID);
  }

  /**
   * Finish the fetch item anyway, even if it's failed to download the target page
   * */
  public void finishFetchItem(FetchItem fetchItem) {
    fetchItemQueues.finishFetchItem(fetchItem);
    lastTaskFinishTime.set(System.currentTimeMillis());
  }

  /**
   * Finished downloading the web page
   * */
  public void finishFetchItem(String queueID, long itemID, ProtocolOutput output) {
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

  public void reviewPendingFetchItems(boolean force) {
    fetchItemQueues.reviewPendingFetchItems(force);
  }

  @SuppressWarnings("rawtypes")
  public float waitAndReport(Context context, int reportIntervalSec, boolean feederAlive) throws IOException {
    // Used for threshold check, holds pages and bytes processed in the last sec
    float pagesLastSec = pages.get();
    long bytesLastSec = bytes.get();

    try {
      Thread.sleep(reportIntervalSec * 1000);
    } catch (InterruptedException e) {}

    pagesLastSec = (pages.get() - pagesLastSec) / reportIntervalSec;
    bytesLastSec = (bytes.get() - bytesLastSec) / reportIntervalSec;
    int readyFetchItems = getReadyItemCount();
    int pendingFetchItems = getPendingItemCount();

    counter.setValue(Counter.readyFetchItems, readyFetchItems);
    counter.setValue(Counter.pendingFetchItems, pendingFetchItems);
    counter.setValue(Counter.activeFetcherThreads, activeFetcherThreads.get());
    counter.setValue(Counter.waitingFetcherThreads, waitingFetcherThreads.get());

    reportAndLogStatus(context, pagesLastSec, bytesLastSec, readyFetchItems, pendingFetchItems);

    final int dumpLimit = 5;
    if (!feederAlive && readyFetchItems <= dumpLimit) {
      dumpFetchItemQueues(dumpLimit);
    }

    if (!feederAlive) {
      int hitByTimeLimit = checkTimelimit();
      if (hitByTimeLimit != 0) {
        counter.increase(Counter.expiredQueues, hitByTimeLimit);
      }
    }

    return pagesLastSec;
  }

  @SuppressWarnings("rawtypes")
  private void reportAndLogStatus(Context context, float pagesPerSec,
      long bytesPerSec, int readyFetchItems, int pendingFetchItems) throws IOException {

    this.avePageLength = bytesPerSec / pagesPerSec;

    StringBuilder status = new StringBuilder();
    long elapsed = (System.currentTimeMillis() - startTime)/1000;

    status.append(waitingFetcherThreads).append("/").append(activeFetcherThreads).append(" waiting/active threads, ");
    status.append(pages).append(" pages, ").append(errors).append(" errors, ");
    status.append(Math.round((((float)pages.get())*10)/elapsed)/10.0).append(" ");
    status.append(Math.round((pagesPerSec*10)/10.0)).append(" pages/s, ");
    status.append(Math.round((((float)bytes.get())*8)/1024)/elapsed).append(" ");
    status.append(Math.round(((float)bytesPerSec)*8)/1024).append(" kb/s, ");

    status.append(readyFetchItems).append(" ready ");
    status.append(pendingFetchItems).append(" pending ");
    status.append("URLs in ").append(getQueueCount()).append(" queues");

//    context.setStatus(statusString);

    LOG.info(status.toString());
    // LOG.info(counter.getStatusString());
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
      if (!skipTruncated || (skipTruncated && !ParserMapper.isTruncated(url, page))) {
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

    pages.incrementAndGet();
    bytes.addAndGet(bytesInPage);
  }

  private void logFetchFailure(String url, String message) {
    LOG.warn("Failed to fetch, " + message);
    errors.incrementAndGet();
    counter.increase(Counter.errors);
  }
}
