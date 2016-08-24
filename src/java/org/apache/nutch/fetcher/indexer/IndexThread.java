package org.apache.nutch.fetcher.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.fetcher.data.FetchItem;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class picks items from queues and fetches the pages.
 * */
public class IndexThread extends Thread {

  public static final Logger LOG = FetcherJob.LOG;

  private static AtomicInteger threadSequence = new AtomicInteger(0);

  private final Configuration conf;

  /** Index just in time */
  private final IndexManager indexManager;

  public IndexThread(IndexManager indexManager, Context context) {
    this.conf = context.getConfiguration();
    this.indexManager = indexManager;

    this.setDaemon(true);
    this.setName("IndexThread-" + threadSequence.incrementAndGet());
  }

  @Override
  public void run() {
    while (!indexManager.isHalted()) {
      try {
        FetchItem item = indexManager.consume();
        if (item != null) {
          indexManager.index(item);
        }
      }
      catch (Exception e) {
        LOG.error("Index thread failed, " + e.toString());
        LOG.error(StringUtil.stringifyException(e));
      }
    }
  } // run
} // FetcherThread
