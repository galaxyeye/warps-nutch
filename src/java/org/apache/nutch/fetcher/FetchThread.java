package org.apache.nutch.fetcher;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.nutch.fetcher.data.FetchItem;
import org.apache.nutch.fetcher.data.FetchResult;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

/**
 * This class picks items from queues and fetches the pages.
 * 
 */
public class FetchThread extends Thread {

  public static final Logger LOG = FetcherJob.LOG;

  private static AtomicInteger fetchThreadSequence = new AtomicInteger(0);

  private final Configuration conf;
  private final ProtocolFactory protocolFactory;
  private String reprUrl;
  private final FetchMode fetchMode;
  private boolean debugContent = false;

  private QueueFeederThread queueFeederThread;
  private final FetchManager fetchManager;

  public FetchThread(QueueFeederThread queueFeederThread, FetchManager fetchManager, Context context) {
    this.conf = context.getConfiguration();

    this.queueFeederThread = queueFeederThread;
    this.fetchManager = fetchManager;

    this.setDaemon(true);
    this.setName("FetcherThread-" + fetchThreadSequence.incrementAndGet());

    this.protocolFactory = new ProtocolFactory(conf);
    this.fetchMode = FetchMode.fromString(conf.get("fetcher.fetch.mode", "native"));
    this.debugContent = conf.getBoolean("fetcher.fetch.thread.debug.content", false);
  }

  public String reprUrl() {
    return reprUrl();
  }

  @Override
  public void run() {
    fetchManager.activeFetcherThreads.incrementAndGet(); // count threads

    FetchItem item = null;

    try {
      while (true) {
        FetchResult result = null;
        item = null;

        if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
          result = fetchManager.consumeFetchResut();

          if (result != null) {
            item = fetchManager.getPendingFetchItem(result.getQueueId(), result.getItemId());

            if (item == null) {
              LOG.debug("Bad fetch item id {}-{}", result.getQueueId(), result.getItemId());
            }
          }
        }
        else {
          item = fetchManager.consumeFetchItem();
        }

        if (item == null) {
          if (!isMissionComplete()) {

            fetchManager.waitingFetcherThreads.incrementAndGet();

            try {
              Thread.sleep(1000);
            } catch (final Exception e) {}

            fetchManager.waitingFetcherThreads.decrementAndGet();

            continue;
          } else {
            // all done, finish this thread
            return;
          }
        }

        Protocol protocol = null;

        if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
          protocol = protocolFactory.getCustomProtocol("crowd://" + item.getQueueID() + "/" + item.getItemID());

          protocol.setResult(result.getStatusCode(), result.getHeaders(), result.getContent());

          if (debugContent) {
            debugContent(result.getUrl(), result.getContent());
          }
        } else {
          if (item.getPage().getReprUrl() == null) {
            reprUrl = item.getUrl();
          } else {
            reprUrl = TableUtil.toString(item.getPage().getReprUrl());
          }

          LOG.debug("fetch {}", item.getUrl());

          // Block, open the network to fetch the web page and wait for a response
          protocol = this.protocolFactory.getProtocol(item.getUrl());
        }

        // Blocking until the target web page is loaded
        final ProtocolOutput output = protocol.getProtocolOutput(item.getUrl(), item.getPage());
        fetchManager.finishFetchItem(item.getQueueID(), item.getItemID(), output);
      } // while
    } catch (final Throwable e) {
      LOG.error("fetcher throwable caught", e);
    } finally {
      if (item != null) fetchManager.finishFetchItem(item);
      fetchManager.activeFetcherThreads.decrementAndGet(); // count threads
      LOG.info("-finishing thread " + getName() + ", activeFetcherThreads=" + fetchManager.activeFetcherThreads);
    }
  } // run

  private boolean isFeederAlive() {
    Validate.notNull(queueFeederThread);

    return queueFeederThread.isAlive();
  }

  private boolean isMissionComplete() {
    return !isFeederAlive() 
        && fetchManager.getReadyItemCount() == 0 
        && fetchManager.getPendingItemCount() == 0;
  }

  private void debugContent(String url, byte[] pageContent) throws IOException {
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
} // FetcherThread
