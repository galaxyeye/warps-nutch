package org.apache.nutch.fetcher.indexer;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.fetcher.data.FetchItem;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexWriters;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by vincent on 16-8-23.
 */
public class IndexManager {

  public static final Logger LOG = FetcherJob.LOG;

  private Configuration conf;

  private int batchSize = 2500;

  // private final BlockingQueue<FetchItem> queue = Queues.newLinkedBlockingQueue(batchSize);
  private final BlockingQueue<FetchItem> queue = Queues.newLinkedBlockingQueue(batchSize);
  private final IndexWriters indexWriters;

  private final LinkedList<IndexThread> indexThreads = Lists.newLinkedList();
  private AtomicBoolean halt = new AtomicBoolean(false);

  public IndexManager(Configuration conf) throws IOException {
    this.conf = conf;

    this.batchSize = conf.getInt("indexer.index.batch.size", this.batchSize);

    indexWriters = new IndexWriters(conf);
    indexWriters.open(conf);
  }

  public void halt() {
    halt.set(true);
    handleHalt();
  }

  public boolean isHalted() {
    return halt.get();
  }

  public void handleHalt() {
    if (!isHalted()) {
      LOG.warn("Invalid IndexManager status, should be halted");
      return;
    }

    LOG.info("IndexManager set to halted, we now index all the rest pages and then exit.");
    LOG.info("There are " + queue.size() + " remainders");

    try {
      FetchItem item = consume();
      while(item != null) {
        index(item);
        item = consume();
      }

      synchronized (indexWriters) {
        indexWriters.commit();
      }
    }
    catch (Exception e) {
      LOG.error(e.toString());
    }
    finally {
      indexWriters.close();
    }

    LOG.info("All done, exit index thread");
  }

  /**
   * thread safety
   * */
  public void index(FetchItem fetchItem) {
    try {
      String url = fetchItem.getUrl();
      String key = TableUtil.reverseUrl(url);
      WebPage page = fetchItem.getPage();

      if (key != null && page != null) {
        IndexDocument doc = new IndexDocument.Builder(conf).build(key, page);
        if (doc != null) {
          synchronized (indexWriters) {
            indexWriters.write(doc);
          }
        } // if
      } // if
    }
    catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * Add fetch item to index queue
   * Thread safe
   * */
  public void produce(FetchItem item) {
    if (isHalted()) {
      return;
    }

    ParseStatus pstatus = item.getPage().getParseStatus();
    if (pstatus == null || !isParseSuccess(pstatus) || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
      // getCounter().increase(IndexingMapper.Counter.unmatchStatus);
      return; // filter urls not parsed
    }

    queue.add(item);
  }

  /**
   * Thread safe
   * */
  public FetchItem consume() {
    return queue.poll();
  }

  private static boolean isParseSuccess(ParseStatus status) {
    if (status == null) {
      return false;
    }
    return status.getMajorCode() == ParseStatusCodes.SUCCESS;
  }
}
