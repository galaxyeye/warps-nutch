package org.apache.nutch.fetch.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetch.FetchMonitor;
import org.apache.nutch.fetch.data.FetchTask;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class picks items from queues and fetches the pages.
 * */
public class IndexThread extends Thread implements Comparable<IndexThread> {

  public static final Logger LOG = FetchMonitor.LOG;

  private static AtomicInteger instanceSequence = new AtomicInteger(0);

  private final Configuration conf;

  private final int id;

  private AtomicBoolean halt = new AtomicBoolean(false);

  private final JITIndexer JITIndexer;

  public IndexThread(JITIndexer JITIndexer, Configuration conf) {
    this.conf = conf;
    this.JITIndexer = JITIndexer;

    this.id = instanceSequence.incrementAndGet();

    this.setDaemon(true);
    this.setName("IndexThread-" + id);
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
        if (item != null && item.getPage() != null) {
          JITIndexer.index(item);
        }
      }
      catch (Exception e) {
        LOG.error("Indexer failed, " + e.toString());
      }
    }

    JITIndexer.unregisterFetchThread(this);
  } // run

  @Override
  public int compareTo(IndexThread indexThread) {
    return getName().compareTo(indexThread.getName());
  }
} // FetcherThread
