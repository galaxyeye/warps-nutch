package org.apache.nutch.fetcher.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetcher.FetchMode;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.host.HostDb;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Convenience class - a collection of queues that keeps track of the total
 * number of items, and provides items eligible for fetching from any queue.
 */
public class FetchItemQueues {
  public static final Logger LOG = FetcherJob.LOG;

  public static final String QUEUE_MODE_HOST = "byHost";
  public static final String QUEUE_MODE_DOMAIN = "byDomain";
  public static final String QUEUE_MODE_IP = "byIP";

  private Map<String, FetchItemQueue> queues = new HashMap<String, FetchItemQueue>();
  private AtomicInteger readyItemCount = new AtomicInteger(0);
  private AtomicInteger pendingItemCount = new AtomicInteger(0);
  private Configuration conf;

  private int maxThreads;
  private String queueMode;
  private long crawlDelay;
  private long minCrawlDelay;
  /**
   * Once timeout, the pending items should be put to the ready queue again.
   * */
  private long pendingTimeout = 3 * 60 * 1000;

  boolean useHostSettings = false;
  HostDb hostDb = null;

  public FetchItemQueues(Configuration conf) throws IOException {
    this.conf = conf;
    this.maxThreads = conf.getInt("fetcher.threads.per.queue", 1);
    String fetchMode = conf.get("fetcher.fetch.mode", "native");
    if (FetchMode.CROWDSOURCING.equals(fetchMode)) {
      this.maxThreads = Integer.MAX_VALUE;
    }
    queueMode = conf.get("fetcher.queue.mode", QUEUE_MODE_HOST);

    // check that the mode is known
    if (!queueMode.equals(QUEUE_MODE_IP) && !queueMode.equals(QUEUE_MODE_DOMAIN)
        && !queueMode.equals(QUEUE_MODE_HOST)) {
      LOG.error("Unknown partition mode : " + queueMode + " - forcing to byHost");
      queueMode = QUEUE_MODE_HOST;
    }

    LOG.info("Using queue mode : " + queueMode);

    // Optionally enable host specific queue behavior
    if (queueMode.equals(QUEUE_MODE_HOST)) {
      useHostSettings = conf.getBoolean("fetcher.queue.use.host.settings", false);
      if (useHostSettings) {
        LOG.info("Host specific queue settings enabled.");
        // Initialize the HostDb if we need it.
        hostDb = new HostDb(conf);
      }
    }

    this.crawlDelay = (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
    this.minCrawlDelay = (long) (conf.getFloat("fetcher.server.min.delay", 0.0f) * 1000);
    this.pendingTimeout = conf.getLong("fetcher.pending.timeout", 3 * 60 * 1000);
  }

  public synchronized void produceFetchItem(FetchItem item) {
    final FetchItemQueue queue = getFetchItemQueue(item.getQueueID());
    queue.produceFetchItem(item);
    readyItemCount.incrementAndGet();
  }

  public synchronized void produceFetchItem(int jobID, String url, WebPage page) {
    final FetchItem it = FetchItem.create(jobID, url, page, queueMode);
    if (it != null) produceFetchItem(it);
  }

  public synchronized FetchItem consumeFetchItem() {
    final Iterator<Map.Entry<String, FetchItemQueue>> it = queues.entrySet().iterator();

    while (it.hasNext()) {
      final FetchItemQueue queue = it.next().getValue();
      // reap empty queues
      if (queue.getFetchQueueSize() == 0 && queue.getFetchingQueueSize() == 0) {
        it.remove();
        continue;
      }

      final FetchItem item = queue.consumeFetchItem();
      if (item != null) {
        readyItemCount.decrementAndGet();
        pendingItemCount.incrementAndGet();

        if (readyItemCount.get() < 0) {
          LOG.error("Ready item count runs negative");
        }

        return item;
      }
    }

    return null;
  }

  public synchronized void finishFetchItem(String queueID, long itemID, boolean asap) {
    final FetchItemQueue queue = queues.get(queueID);
    if (queue == null) {
      LOG.warn("Attempting to finish item from unknown queue: " + queueID);
      return;
    }

    if (!queue.fetchingItemExist(itemID)) {
      LOG.warn("Attempting to finish unknown item: " + itemID);
      return;
    }

    pendingItemCount.decrementAndGet();
    queue.finishFetchItem(itemID, asap);
  }

  public synchronized void finishFetchItem(FetchItem item) {
    finishFetchItem(item.getQueueID(), item.getItemID(), false);
  }

  public synchronized void finishFetchAsap(FetchItem item) {
    finishFetchItem(item.getQueueID(), item.getItemID(), true);
  }

  /**
   * Reload pending fetch items so that the items can be re-fetched
   * 
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should restart the task
   * 
   * @param force reload all pending fetch items immediately
   * */
  public synchronized void reviewPendingFetchItems(boolean force) {
    int readyCount = 0;
    int pendingCount = 0;

    final Iterator<FetchItemQueue> it = queues.values().iterator();
    while (it.hasNext()) {
      FetchItemQueue queue = it.next();
      queue.reviewPendingFetchItems(force);

      readyCount += queue.getFetchQueueSize();
      pendingCount += queue.getFetchingQueueSize();
    }

    readyItemCount.set(readyCount);
    pendingItemCount.set(pendingCount);
  }

  public synchronized FetchItem getPendingFetchItem(String queueID, long itemID) {
    FetchItemQueue queue = getFetchItemQueue(queueID);

    if (queue == null) return null;

    return queue.getPendingFetchItem(itemID);
  }

  public synchronized void dump(int limit) {
    for (final String id : queues.keySet()) {
      final FetchItemQueue queue = queues.get(id);

      if (queue.getFetchQueueSize() == 0) {
        continue;
      }

      LOG.info("* queue: " + id);
      queue.dump();

      if (--limit < 0) {
        break;
      }
    }
  }

  // empties the queues (used by timebomb and throughput threshold)
  public synchronized int clearQueues() {
    int count = 0;

    // emptying the queues
    for (String id : queues.keySet()) {
      FetchItemQueue queue = queues.get(id);

      if (queue.getFetchQueueSize() == 0) continue;

      LOG.info("* queue: " + id + " >> dropping! ");
      int deleted = queue.clearFetchQueue();
      for (int i = 0; i < deleted; i++) {
        readyItemCount.decrementAndGet();
      }

      count += deleted;
    }

    // there might also be a case where readyItemCount != 0 but number of queues
    // == 0
    // in which case we simply force it to 0 to avoid blocking
    if (readyItemCount.get() != 0 && queues.size() == 0) {
      readyItemCount.set(0);
    }

    return count;
  }

  public synchronized int getQueueCount() {
    return queues.size();
  }

  public int getReadyItemCount() {
    return readyItemCount.get();
  }

  public int getPendingItemCount() {
    return pendingItemCount.get();
  }

  private FetchItemQueue getFetchItemQueue(String id) {
    FetchItemQueue queue = queues.get(id);
    if (queue == null) {
      // Create a new queue
      if (useHostSettings) {
        // Use host specific queue settings (if defined in the host table)
        try {
          String hostname = id.substring(id.indexOf("://") + 3);
          Host host = hostDb.getByHostName(hostname);
          if (host != null) {
            queue = new FetchItemQueue(conf,
                           host.getInt("q_mt", maxThreads),
                           host.getLong("q_cd", crawlDelay),
                           host.getLong("q_mcd", minCrawlDelay), 
                           pendingTimeout);
          }
        } catch (IOException e) {
          LOG.error("Error while trying to access host settings", e);
        }
      }

      if (queue == null) {
        // Use queue defaults
        queue = new FetchItemQueue(conf, maxThreads, crawlDelay, minCrawlDelay, pendingTimeout);
      }

      queues.put(id, queue);
    }

    return queue;
  }
}
