package org.apache.nutch.fetcher.data;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetcher.FetchMode;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.host.HostDb;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps track of the all fetch items
 *
 * TODO : synchronize on workingQueues, not functions
 */
public class FetchItemQueues {
  public static final Logger LOG = FetcherJob.LOG;

  static final String QUEUE_MODE_HOST = "byHost";
  static final String QUEUE_MODE_DOMAIN = "byDomain";
  static final String QUEUE_MODE_IP = "byIP";

  private final Configuration conf;
//  private final Map<String, FetchItemQueue> workingQueues = new HashMap<>();
//  private final PriorityQueue<FetchItemQueue> priorityWorkingQueues = new PriorityQueue<>();
  private final FetchItemPriorityQueue workingQueues = new FetchItemPriorityQueue();

  // private final Map<String, FetchItemQueue> workingQueues = new ConcurrentHashMap<>();
  /**
   * Tracking time cost of each queue
   */
  private final Map<String, Double> queueTimeCosts = new TreeMap<>();
  private final Set<String> unreachableHosts = Sets.newTreeSet();

  private final AtomicInteger readyItemCount = new AtomicInteger(0);
  private final AtomicInteger pendingItemCount = new AtomicInteger(0);
  private final AtomicInteger finishedItemCount = new AtomicInteger(0);

  private final long crawlDelay;
  private final long minCrawlDelay;

  private String queueMode;
  private int maxThreads;
  private boolean useHostSettings = false;

  /**
   * Once timeout, the pending items should be put to the ready queue again.
   */
  private long pendingTimeout = 3 * 60 * 1000;

  private HostDb hostDb = null;

  private int nextQueuePosition = 0;

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

    LOG.info(StringUtil.formatParamsLine(
        "className", this.getClass().getSimpleName(),
        "maxThreadsPerQueue", maxThreads,
        "queueMode", queueMode,
        "useHostSettings", useHostSettings,
        "crawlDelay", crawlDelay,
        "minCrawlDelay", minCrawlDelay,
        "pendingTimeout", pendingTimeout
    ));
  }

  public synchronized void produce(FetchItem item) { doProduce(item, 0); }

  public synchronized void produce(int jobID, String url, WebPage page) {
    final FetchItem it = FetchItem.create(jobID, url, page, queueMode);
    if (it != null) {
      doProduce(it, 0);
    }
  }

  private void doProduce(FetchItem item, int priority) {
    FetchItemQueue queue = getOrCreateFetchItemQueue(item.getQueueID(), priority);
    queue.produce(item);
    readyItemCount.incrementAndGet();
    queueTimeCosts.put(queue.getId(), 0.0);
  }

  public synchronized FetchItem consume(String queueId) {
    FetchItemQueue queue = workingQueues.getOrPeek(queueId);
    return doConsume(queue);
  }

  private FetchItem doConsume(FetchItemQueue queue) {
    if (queue == null) {
      return null;
    }

    FetchItem item = queue.consume(unreachableHosts);

    if (item != null) {
      readyItemCount.decrementAndGet();
      pendingItemCount.incrementAndGet();
    }

    return item;
  }

  public synchronized void finish(String queueId, long itemID, boolean asap) {
    doFinish(queueId, itemID, asap);
  }

  public synchronized void finish(FetchItem item) {
    doFinish(item.getQueueID(), item.getItemID(), false);
  }

  public synchronized void finishAsap(FetchItem item) { finish(item.getQueueID(), item.getItemID(), true); }

  private void doFinish(String queueId, long itemID, boolean asap) {
    FetchItemQueue queue = workingQueues.getMore(queueId);

    if (queue == null) {
      LOG.warn("Attemp to finish item from unknown queue: " + queueId);
      return;
    }

    if (!queue.fetchingItemExist(itemID)) {
      LOG.warn("Attemp to finish unknown item: " + itemID);
      return;
    }

    queue.finish(itemID, asap);
    pendingItemCount.decrementAndGet();
    finishedItemCount.incrementAndGet();

    queueTimeCosts.put(queueId, queue.getAvarageTimeCost());
  }

  public synchronized String getCostReport() {
    return workingQueues.getCostReport();
  }

  /**
   * Reload pending fetch items so that the items can be re-fetched
   * 
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should restart the task
   * 
   * @param force reload all pending fetch items immediately
   * */
  public synchronized void reviewPendingTasks(boolean force) {
    int readyCount = 0;
    int pendingCount = 0;

    for (FetchItemQueue queue : workingQueues.values()) {
      queue.reviewPendingTasks(force);

      readyCount += queue.getFetchQueueSize();
      pendingCount += queue.getFetchingQueueSize();
    }

    readyItemCount.set(readyCount);
    pendingItemCount.set(pendingCount);
  }

  /**
   * Get a pending task, the task can be in working queues or in detached queues
   * */
  public synchronized FetchItem getPendingTask(String queueId, long itemID) {
    return workingQueues.getPendingTask(queueId, itemID);
  }

  public synchronized void dump(int limit) {
    workingQueues.dump(limit);
  }

  public synchronized void addUnreachableHost(String host) {
    if (unreachableHosts.contains(host)) {
      return;
    }

    LOG.warn("Host unknown: " + host);
    final int maxUnreachableHosts = 10000;
    if (unreachableHosts.size() < maxUnreachableHosts) {
      unreachableHosts.add(host);
    }
  }

  public synchronized void reportUnreachableHosts() {
    Set<String> unreachableDomainSet = new HashSet<>(this.unreachableHosts);
    if (!unreachableDomainSet.isEmpty()) {
      String report = StringUtils.join(unreachableDomainSet, '\n');
      report = "Unreachable hosts : \n" + report;
      LOG.info(report);
    }
  }

  public synchronized int clearSlowestQueue() {
    FetchItemQueue queue = getSlowestQueue();
    if (queue == null) {
      return 0;
    }

    workingQueues.detach(queue);

    tryClearVerySlowFetchingTasks(queue, 2);

    return clearReadyTasks(queue);
  }

//  /**
//   * Slow queues do not serve any more
//   * */
//  private void detach(FetchItemQueue queue) {
//    queue.detach();
//    workingQueues.remove(queue.getId());
//    detachedQueues.put(queue.getId(), queue);
//  }

  public synchronized int clearReadyTasks() {
    int count = 0;

    Map<Double, String> costRecorder = Maps.newTreeMap();
    for (String id : workingQueues.keySet()) {
      FetchItemQueue queue = workingQueues.get(id);
      costRecorder.put(queue.getAvarageTimeCost(), queue.getId());

      if (queue.getFetchQueueSize() == 0) {
        continue;
      }

      count += clearReadyTasks(queue);
    }

    reportCost(costRecorder);

    return count;
  }

  private int tryClearVerySlowFetchingTasks(FetchItemQueue queue, int limit) {
    int deleted = queue.tryClearVerySlowFetchingTasks(limit);
    pendingItemCount.addAndGet(0 - deleted);
    return deleted;
  }

  private int clearReadyTasks(FetchItemQueue queue) {
    int deleted = queue.clearFetchQueue();

    readyItemCount.addAndGet(0 - deleted);
    if (readyItemCount.get() <= 0 && workingQueues.size() == 0) {
      readyItemCount.set(0);
    }

    return deleted;
  }

  public synchronized int getQueueCount() {
    return workingQueues.size();
  }

  public int getReadyItemCount() { return readyItemCount.get(); }

  public int getPendingItemCount() { return pendingItemCount.get(); }

  public int getFinishedItemCount() {return finishedItemCount.get(); }

  private FetchItemQueue getSlowestQueue() {
    double maxCost = 0;
    String slowestQueueId = null;

    for (Map.Entry<String, Double> entry : queueTimeCosts.entrySet()) {
      double cost = entry.getValue();
      if (cost > maxCost) {
        maxCost = cost;
        slowestQueueId = entry.getKey();
      }
    }

    if (slowestQueueId == null) {
      return null;
    }

    queueTimeCosts.remove(slowestQueueId);

    FetchItemQueue queue = workingQueues.get(slowestQueueId);
    if (queue == null) {
      return null;
    }

    DecimalFormat df = new DecimalFormat("0.##");
    LOG.warn("Slow queue : " + queue.getId()
        + ", slow task count : " + queue.getSlowTaskCount()
        + ", avarage cost : " + df.format(maxCost / 1000) + "s"
    );

    return queue;
  }

  private FetchItemQueue getOrCreateFetchItemQueue(String queueId, int priority) {
    FetchItemQueue queue = workingQueues.get(queueId);

    if (queue == null) {
      queue = createFetchItemQueue(queueId, priority);
      workingQueues.add(queue);
    }

    return queue;
  }

  private FetchItemQueue createFetchItemQueue(String queueId, int priority) {
    FetchItemQueue queue = null;

    // Create a new queue
    if (useHostSettings) {
      // Use host specific queue settings (if defined in the host table)
      try {
        String hostname = queueId.substring(queueId.indexOf("://") + 3);
        Host host = hostDb.getByHostName(hostname);
        if (host != null) {
          queue = new FetchItemQueue(queueId, priority,
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
      queue = new FetchItemQueue(queueId, priority, maxThreads, crawlDelay, minCrawlDelay, pendingTimeout);
    }

    return queue;
  }

  private void reportCost(Map<Double, String> costRecorder) {
    StringBuilder sb = new StringBuilder();
    DecimalFormat df = new DecimalFormat("###0.00");
    sb.append("Queue cost report : \n");
    int i = 0;
    for (Map.Entry<Double, String> entry : costRecorder.entrySet()) {
      if (i++ < 100) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(df.format(entry.getKey()));
        sb.append(" - ");
        sb.append(entry.getValue());
      }
    }
    LOG.info(sb.toString());
  }
}
