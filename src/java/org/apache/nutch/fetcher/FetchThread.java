package org.apache.nutch.fetcher;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.nutch.fetcher.data.FetchItem;
import org.apache.nutch.fetcher.data.FetchResult;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class picks items from queues and fetches the pages.
 * */
public class FetchThread extends Thread {

  enum FetchStatus { Success, Failed}

  private class FetchData {
    FetchData(FetchItem item, FetchResult result) {
      this.item = item;
      this.result = result;
    }

    FetchResult result;
    FetchItem item;
  }

  public static final Logger LOG = FetcherJob.LOG;

  private static AtomicInteger fetchThreadSequence = new AtomicInteger(0);

  private final Configuration conf;
  private AtomicBoolean halted = new AtomicBoolean(false);

  private final ProtocolFactory protocolFactory;
  private String reprUrl;
  /**
   * Native, Crowdsourcing, Proxy
   * */
  private final FetchMode fetchMode;
  private boolean debugContent = false;

  private QueueFeederThread queueFeederThread;
  private final FetchManager fetchManager;

  /** Fix the thread to a specified queue as possible as we can */
  private String currQueueId;
  private Set<String> servedQueues = new HashSet<>();

  public FetchThread(QueueFeederThread queueFeederThread, FetchManager fetchManager, Context context) {
    this.conf = context.getConfiguration();

    this.queueFeederThread = queueFeederThread;
    this.fetchManager = fetchManager;

    this.setDaemon(true);
    this.setName(getClass().getSimpleName() + "-" + fetchThreadSequence.incrementAndGet());

    this.protocolFactory = new ProtocolFactory(conf);
    this.fetchMode = FetchMode.fromString(conf.get("fetcher.fetch.mode", "native"));
    this.debugContent = conf.getBoolean("fetcher.fetch.thread.debug.content", false);

//    LOG.info(StringUtil.formatParams(
//        "className", this.getClass().getSimpleName()
//    ));
  }

  public String reprUrl() { return reprUrl; }

  public void halt() { halted.set(true); }

  public boolean isHalted() { return halted.get(); }

  @Override
  public void run() {
    fetchManager.registerFetchThread(this);

    FetchData fetchData = null;

    try {
      while (!isMissionComplete() && !isHalted()) {
        fetchData = getFetchData();

        if (fetchData.item == null) {
          sleepAndRecord();
          continue;
        }

        fetchOne(fetchData);
      } // while
    } catch (final Throwable e) {
      LOG.error("Fetcher throwable caught" + e.toString());
    } finally {
      if (fetchData != null && fetchData.item != null) {
        fetchManager.finishFetchItem(fetchData.item);
      }

      fetchManager.unregisterFetchThread(this);

      LOG.info("Thread " + getName() + " finished, still active threads : " + fetchManager.getActiveFetchThreadCount());
      // LOG.info("Served queues : " + StringUtils.join());
    }
  } // run

  private void sleepAndRecord() {
    fetchManager.registerIdleThread(this);

    try {
      // TODO : use a smarter algorithm
      Thread.sleep(2000);
    } catch (final Exception e) {}

    fetchManager.unregisterIdleThread(this);
  }

  private FetchData getFetchData() {
    FetchResult fetchResult = null;
    FetchItem fetchItem = null;

    if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
      fetchResult = fetchManager.consumeFetchResut();

      if (fetchResult != null) {
        fetchItem = fetchManager.getPendingFetchItem(fetchResult.getQueueId(), fetchResult.getItemId());

        if (fetchItem == null) {
          LOG.warn("Bad fetch item id {}-{}", fetchResult.getQueueId(), fetchResult.getItemId());
        }
      }
    }
    else {
      fetchItem = fetchManager.consumeFetchItem(currQueueId);
    }

    if (fetchItem != null) {
      currQueueId = fetchItem.getQueueID();
      servedQueues.add(currQueueId);
    }

    return new FetchData(fetchItem, fetchResult);
  }

  /**
   * Fetch one web page
   * */
  private FetchStatus fetchOne(FetchData fetchData) throws ProtocolNotFound, IOException {
    Protocol protocol = getProtocol(fetchData);

    FetchItem item = fetchData.item;
    FetchResult result = fetchData.result;

    if (item == null) {
      return FetchStatus.Failed;
    }

    // Blocking until the target web page is loaded
    final ProtocolOutput output = protocol.getProtocolOutput(item.getUrl(), item.getPage());
    fetchManager.finishFetchItem(item.getQueueID(), item.getItemID(), output);

    if (debugContent) {
      debugContent(result.getUrl(), result.getContent());
    }

    return FetchStatus.Success;
  }

  /**
   * Get network protocol, for example : http, ftp, sftp, and crowd protocol, etc
   *
   * */
  private Protocol getProtocol(FetchData fetchData) throws ProtocolNotFound {
    FetchItem item = fetchData.item;
    FetchResult result = fetchData.result;

    Protocol protocol;
    if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
      protocol = protocolFactory.getCustomProtocol("crowd://" + item.getQueueID() + "/" + item.getItemID());

      protocol.setResult(result.getStatusCode(), result.getHeaders(), result.getContent());
    } else {
      if (item.getPage().getReprUrl() == null) {
        reprUrl = item.getUrl();
      } else {
        reprUrl = TableUtil.toString(item.getPage().getReprUrl());
      }

      // Block, open the network to fetch the web page and wait for a response
      protocol = this.protocolFactory.getProtocol(item.getUrl());
    }

    return protocol;
  }

  private boolean isFeederAlive() {
    return queueFeederThread != null && queueFeederThread.isAlive();
  }

  private boolean isMissionComplete() {
    return !isFeederAlive()
        && fetchManager.getReadyItemCount() == 0
        && fetchManager.getPendingItemCount() == 0;
  }

  private void debugContent(String url, byte[] pageContent) {
    try {
      File dir = new File("/tmp/fetcher/");
      if (!dir.exists()) {
        FileUtils.forceMkdir(dir);
      }

      // make a clean up every hour
      String date = new SimpleDateFormat("mm").format(new Date());
      if (date.equals("00")) {
        Files.deleteIfExists(dir.toPath());
      }

      File file = new File("/tmp/fetcher/" + DigestUtils.md5Hex(url) + ".html");
      if (!file.exists()) {
        file.createNewFile();
      }

      FileUtils.writeByteArrayToFile(file, pageContent);
    }
    catch (IOException e) {
      LOG.error(e.toString());
    }
  }

} // FetcherThread
