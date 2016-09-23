package org.apache.nutch.fetcher.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.nutch.fetcher.FetchJob;
import org.apache.nutch.fetcher.data.FetchTask;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class picks items from queues and fetches the pages.
 * */
public class IndexThread extends Thread implements Comparable<IndexThread> {

  public static final Logger LOG = FetchJob.LOG;

  private static AtomicInteger instanceSequence = new AtomicInteger(0);

  private final Configuration conf;
  private final int processLevelId;

  private AtomicBoolean halt = new AtomicBoolean(false);

  private final JITIndexer JITIndexer;

  public IndexThread(JITIndexer JITIndexer, Context context) {
    this.conf = context.getConfiguration();
    this.JITIndexer = JITIndexer;

    this.processLevelId = instanceSequence.incrementAndGet();

    this.setDaemon(true);
    this.setName("IndexThread-" + processLevelId);
  }

  public void halt() {
    halt.set(true);
  }

  public boolean isHalted() {
    return halt.get();
  }

  @Override
  public void run() {
    JITIndexer.registerFetchThread(this);

    while (!isHalted()) {
      try {
        FetchTask item = JITIndexer.consume();
        if (item != null) {
          JITIndexer.index(item);
        }
      }
      catch (Exception e) {
        LOG.error("Index thread failed, " + e.toString());
      }
    }

    JITIndexer.unregisterFetchThread(this);
  } // run

  @Override
  public int compareTo(IndexThread indexThread) {
    return getName().compareTo(indexThread.getName());
  }
} // FetcherThread
