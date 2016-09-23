package org.apache.nutch.fetcher;

import com.google.common.collect.Sets;
import org.apache.commons.collections4.SortedBidiMap;
import org.apache.commons.collections4.bidimap.DualTreeBidiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetcher.data.FetchQueue;
import org.apache.nutch.fetcher.data.FetchQueues;
import org.apache.nutch.fetcher.data.FetchTask;
import org.apache.nutch.host.HostDb;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nutch.fetcher.FetchScheduler.QUEUE_REMAINDER_LIMIT;
import static org.apache.nutch.fetcher.data.FetchQueues.*;

/**
 * Keeps track of the all fetch items
 *
 * TODO : synchronize on workingQueues, not functions
 */
public class FetchMonitor {
  public static final Logger LOG = FetchJob.LOG;

  private final Configuration conf;
  private final FetchQueues workingQueues = new FetchQueues();

  /**
   * Tracking time cost of each queue
   */
  private final SortedBidiMap<String, Double> queueTimeCosts = new DualTreeBidiMap<>();
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

  public FetchMonitor(Configuration conf) throws IOException {
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

    DecimalFormat df = new DecimalFormat("###0.0#");
    LOG.info(StringUtil.formatParamsLine(
        "className", this.getClass().getSimpleName(),
        "maxThreadsPerQueue", maxThreads,
        "queueMode", queueMode,
        "useHostSettings", useHostSettings,
        "crawlDelay(n)", df.format(crawlDelay / 60.0 / 1000.0),
        "minCrawlDelay(m)", df.format(minCrawlDelay / 60.0 / 1000.0),
        "pendingTimeout(m)", df.format(pendingTimeout / 60.0 / 1000.0)
    ));
  }

  public synchronized void produce(FetchTask item) {
    doProduce(item, 0);
  }

  public synchronized void produce(int jobID, String url, WebPage page) {
    final FetchTask it = FetchTask.create(jobID, url, page, queueMode);
    if (it != null) {
      doProduce(it, 0);
    }
  }

  private void doProduce(FetchTask item, int priority) {
    FetchQueue queue = getOrCreateFetchQueue(item.getQueueID(), priority);
    queue.produce(item);
    readyItemCount.incrementAndGet();
    queueTimeCosts.put(queue.getId(), 0.0);
  }

  public synchronized FetchTask consume(String queueId) {
    FetchQueue queue = workingQueues.getOrPeek(queueId);
    return doConsume(queue);
  }

  private FetchTask doConsume(FetchQueue queue) {
    if (queue == null) {
      return null;
    }

    FetchTask item = queue.consume(unreachableHosts);

    if (item != null) {
      readyItemCount.decrementAndGet();
      pendingItemCount.incrementAndGet();
    }

    return item;
  }

  public synchronized void finish(String queueId, int itemId, boolean asap) {
    doFinish(queueId, itemId, asap);
  }

  public synchronized void finish(FetchTask item) {
    doFinish(item.getQueueID(), item.getItemID(), false);
  }

  public synchronized void finishAsap(FetchTask item) {
    finish(item.getQueueID(), item.getItemID(), true);
  }

  private void doFinish(String queueId, int itemId, boolean asap) {
    FetchQueue queue = workingQueues.getMore(queueId);

    if (queue == null) {
      LOG.warn("Attemp to finish item from unknown queue: " + queueId);
      return;
    }

    if (!queue.pendingItemExist(itemId)) {
      LOG.warn("Attemp to finish unknown item: [{} - {}]", queueId, itemId);
      return;
    }

    queue.finish(itemId, asap);
    pendingItemCount.decrementAndGet();
    finishedItemCount.incrementAndGet();

    queueTimeCosts.put(queueId, queue.avarageTimeCost());
  }

  public synchronized void report() {
    dump(QUEUE_REMAINDER_LIMIT);

    reportUnreachableHosts();

    reportCost();
  }

  private void reportCost() {
    StringBuilder sb = new StringBuilder();
    sb.append("Top slow hosts\n");
    sb.append(workingQueues.getCostReport());
    LOG.info(sb.toString());
  }

  /**
   * Reload pending fetch items so that the items can be re-fetched
   * <p>
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should restart the task
   *
   * @param force reload all pending fetch items immediately
   */
  public synchronized void review(boolean force) {
    int readyCount = 0;
    int pendingCount = 0;

    for (FetchQueue queue : workingQueues.values()) {
      queue.review(force);

      readyCount += queue.readyCount();
      pendingCount += queue.pendingCount();
    }

    readyItemCount.set(readyCount);
    pendingItemCount.set(pendingCount);
  }

  /**
   * Get a pending task, the task can be in working queues or in detached queues
   */
  public synchronized FetchTask getPendingTask(String queueId, int itemID) {
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
      report = "Unreachable hosts : \n" + report + "\n";
      LOG.info(report);
    }
  }

  public synchronized int clearSlowestQueue() {
    FetchQueue queue = getSlowestQueue();
    if (queue == null) {
      return 0;
    }

    workingQueues.detach(queue);

    tryClearVerySlowFetchingTasks(queue, 2);

    int deleted = clearReadyTasks(queue);

    DecimalFormat df = new DecimalFormat("0.##");
    LOG.warn("Slowest queue detached : " + queue.getId()
        + ", slow task count : " + queue.getSlowTaskCount()
        + ", avarage cost : " + df.format(queue.avarageTimeCost() / 1000) + "s"
        + ", deleted : " + deleted
    );

    return deleted;
  }

  public synchronized void cleanup() {
    workingQueues.clear();
    readyItemCount.set(0);
  }

  public synchronized int clearReadyTasks() {
    int count = 0;

    Map<Double, String> costRecorder = new TreeMap<>(Comparator.reverseOrder());
    for (String queueId : workingQueues.keySet()) {
      FetchQueue queue = workingQueues.get(queueId);
      costRecorder.put(queue.avarageTimeCost(), queue.getId());

      if (queue.readyCount() == 0) {
        continue;
      }

      count += clearReadyTasks(queue);
    }

    reportCost(costRecorder);

    return count;
  }

  private int tryClearVerySlowFetchingTasks(FetchQueue queue, int limit) {
    int deleted = queue.tryClearVerySlowFetchingTasks(limit);
    pendingItemCount.addAndGet(0 - deleted);
    return deleted;
  }

  private int clearReadyTasks(FetchQueue queue) {
    int deleted = queue.clearReadyQueue();

    readyItemCount.addAndGet(0 - deleted);
    if (readyItemCount.get() <= 0 && workingQueues.size() == 0) {
      readyItemCount.set(0);
    }

    return deleted;
  }

  public synchronized int getQueueCount() {
    return workingQueues.size();
  }

  public int readyItemCount() { return readyItemCount.get(); }

  public int pendingItemCount() { return pendingItemCount.get(); }

  public int getFinishedItemCount() {return finishedItemCount.get(); }

  private FetchQueue getSlowestQueue() {
    FetchQueue queue = null;

    while (!workingQueues.isEmpty() && queue == null) {
      double maxCost = queueTimeCosts.inverseBidiMap().lastKey();
      String slowestQueueId = queueTimeCosts.inverseBidiMap().get(maxCost);
      queueTimeCosts.remove(slowestQueueId);

      queue = workingQueues.get(slowestQueueId);
    }

    return queue;
  }

  private FetchQueue getOrCreateFetchQueue(String queueId, int priority) {
    FetchQueue queue = workingQueues.get(queueId);

    if (queue == null) {
      queue = createFetchQueue(queueId, priority);
      workingQueues.add(queue);
    }

    return queue;
  }

  private FetchQueue createFetchQueue(String queueId, int priority) {
    FetchQueue queue = null;

    // Create a new queue
    if (useHostSettings) {
      // Use host specific queue settings (if defined in the host table)
      try {
        String hostname = queueId.substring(queueId.indexOf("://") + 3);
        Host host = hostDb.getByHostName(hostname);
        if (host != null) {
          queue = new FetchQueue(queueId,
              priority,
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
      queue = new FetchQueue(queueId, priority, maxThreads, crawlDelay, minCrawlDelay, pendingTimeout);
    }

    return queue;
  }

  private void reportCost(Map<Double, String> costRecorder) {
    StringBuilder sb = new StringBuilder();

    sb.append(String.format("\n%s\n", "---------------Queue Cost Report--------------"));
    sb.append(String.format("%25s %s\n", "Ava Time(s)", "Queue Id"));
    int i = 0;
    for (Map.Entry<Double, String> entry : costRecorder.entrySet()) {
      if (i++ > 100) {
        break;
      }

      sb.append(String.format("%1$,4d.%1$,20.2f", i, entry.getKey()));
      sb.append(" <- ");
      sb.append(entry.getValue());
      sb.append("\n");
    }

    LOG.info(sb.toString());
  }
}
