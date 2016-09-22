package org.apache.nutch.fetcher.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class handles FetchItems which come from the same host ID (be it
 * a proto/hostname or proto/IP pair).
 *
 * It also keeps track of requests in progress and elapsed time between requests.
 */
class FetchItemQueue implements Comparable<FetchItemQueue> {
  public static final Logger LOG = FetcherJob.LOG;

  /** Queue id */
  private final String id;
  /** Queue priority */
  private final int priority;

  private final long crawlDelay;    // millisecond
  private final long minCrawlDelay; // millisecond
  /** Max thread count for this queue */
  private final int maxThreads;
  /** If a task costs more then 10 milliseconds, it's a slow task */
  private final long slowTaskThreshold = 10;

  /** Hold all tasks ready to fetch */
  private final List<FetchItem> fetchQueue = new LinkedList<>();
  /** Hold all tasks are fetching */
  private final Map<Long, FetchItem>  fetchingQueue = new HashMap<>();
  /**
   * Once timeout, the pending items should be put to the ready queue again.
   * time unit : seconds
   * */
  private final long pendingTimeout;
  private final AtomicLong nextFetchTime = new AtomicLong();

  /** Record timing cost of slow tasks */
  private final Set<Long> costRecorder = Sets.newTreeSet();
  private int totalTasks = 1;
  private int totalFetchTime = 0;

  /**
   * Detached from the system, the queue does not accept any tasks, nor serve any requests,
   * but still hold pending tasks
   * */
  private boolean detached = false;

  public FetchItemQueue(String id, int priority, int maxThreads, long crawlDelay, long minCrawlDelay, long pendingTimeout) {
    this.id = id;
    this.priority = priority;
    this.maxThreads = maxThreads;
    this.crawlDelay = crawlDelay;
    this.minCrawlDelay = minCrawlDelay;
    this.pendingTimeout = pendingTimeout;

    // ready to start
    setEndTime(System.currentTimeMillis() - crawlDelay);
  }

  public String getId() { return id; }

  public int getPriority() { return priority; }

  public void produce(FetchItem item) {
    if (item == null || detached) {
      return;
    }

    fetchQueue.add(item);
  }

  public FetchItem consume(Set<String> unreachableHosts) {
    if (detached) {
      return null;
    }

    if (fetchingQueue.size() >= maxThreads) {
      return null;
    }

    final long now = System.currentTimeMillis();
    if (nextFetchTime.get() > now) {
      return null;
    }

    if (fetchQueue.isEmpty()) {
      return null;
    }

    FetchItem item = null;
    try {
      item = fetchQueue.remove(0);

      // Check whether it's unreachable
      if (!unreachableHosts.isEmpty()) {
        // if (queueMode.equals(byDomain))
        String domain = URLUtil.getDomainName(item.getUrl());
        while (unreachableHosts.contains(domain) && !fetchQueue.isEmpty()) {
          item = fetchQueue.remove(0);
          domain = URLUtil.getDomainName(item.getUrl());
        }
      }

      if (item != null) {
        addToFetchingQueue(item, now);
      }
    } catch (final Exception e) {
      LOG.error("Failed to comsume fetch task, {}", e);
    }

    return item;
  }

  /**
   *
   * Note : We have set response time for each page, @see {HttpBase#getProtocolOutput}
   * */
  public void finish(FetchItem item, boolean asap) {
    fetchingQueue.remove(item.getItemID());

    long endTime = System.currentTimeMillis();
    setEndTime(endTime, asap);

    // Record fetch time cost
    long fetchTime = endTime - item.getPendingStartTime();

    if (fetchTime > slowTaskThreshold) {
      if (costRecorder.size() > 100) {
        costRecorder.clear();
      }

      costRecorder.add(fetchTime);
    }

    ++totalTasks;
    totalFetchTime += fetchTime;
    if (totalTasks > 100) {
      totalTasks = 0;
      totalFetchTime = 0;
    }
  }

  public void finish(long itemID, boolean asap) {
    FetchItem item = fetchingQueue.get(itemID);
    if (item != null) {
      finish(item, asap);
    }
  }

  /**
   * Reload pending fetch items so that the items can be re-fetched
   *
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should restart the task
   *
   * @param force reload all pending fetch items immediately
   * */
  public void reviewPendingTasks(boolean force) {
    long now = System.currentTimeMillis();

    List<FetchItem> readyList = Lists.newArrayList();
    Map<Long, FetchItem> pendingList = Maps.newHashMap();

    final Iterator<FetchItem> it = fetchingQueue.values().iterator();
    while (it.hasNext()) {
      FetchItem item = it.next();

      if (force || now - item.getPendingStartTime() > this.pendingTimeout) {
        readyList.add(item);
      }
      else {
        pendingList.put(item.getItemID(), item);
      }
    }

    fetchingQueue.clear();
    fetchQueue.addAll(readyList);
    fetchingQueue.putAll(pendingList);
  }

  public FetchItem getPendingTask(long itemID) {
    return fetchingQueue.get(itemID);
  }

  public int getSlowTaskCount() {
    return costRecorder.size();
  }

  public boolean isSlow() {
    return isSlow(1);
  }

  public boolean isSlow(int costInSec) {
    return getAvarageTimeCost() < costInSec * 1000;
  }

  /**
   * Avarage cost in milliseconds
   * */
  public double getAvarageTimeCost() {
    return (double) totalFetchTime / totalTasks;
  }

  public String getCostReport() {
    final DecimalFormat df = new DecimalFormat("0.##");

    StringBuilder sb = new StringBuilder();
    sb.append(id);
    sb.append(" -> ");
    sb.append(df.format(getAvarageTimeCost() / 1000));
    sb.append('s');

    return sb.toString();
  }

  public int getFetchQueueSize() { return fetchQueue.size(); }

  public int getFetchingQueueSize() {
    return fetchingQueue.size();
  }

  public long getCrawlDelay() {
    return crawlDelay;
  }

  public boolean fetchingItemExist(long itemID) {
    return fetchingQueue.containsKey(itemID);
  }

  public void detach() { this.detached = true; }

  public boolean isDetached() { return this.detached; }

  public int clearFetchQueue() {
    int count = fetchQueue.size();
    fetchQueue.clear();
    return count;
  }

  public int tryClearVerySlowFetchingTasks(int limit) {
    int count = fetchingQueue.size();

    if (count > limit) {
      return 0;
    }

    if (fetchingQueue.isEmpty()) {
      return 0;
    }

    LOG.info("Clear fetching items (might be very slow) : ");
    final DecimalFormat df = new DecimalFormat("###0.##");
    long now = System.currentTimeMillis();

    fetchingQueue.values().stream().limit(limit).filter(Objects::nonNull).forEach(fetchItem -> {
      double elapsed = (now - fetchItem.getPendingStartTime()) / 1000;
      LOG.info(String.format("%s -> pending for %s", fetchItem.getUrl(), df.format(elapsed)));
    });

    fetchingQueue.clear();
    return count;
  }

  public void dump() {
    final DecimalFormat df = new DecimalFormat("###0.##");

    LOG.info("  id            = " + id);
    LOG.info("  maxThreads    = " + maxThreads);
    LOG.info("  fetchingQueue = " + fetchingQueue.size());
    LOG.info("  crawlDelay(s) = " + df.format(crawlDelay / 1000));
    LOG.info("  minCrawlDelay(s) = " + df.format(minCrawlDelay / 1000));
    LOG.info("  now           = " + TimingUtil.now());
    LOG.info("  nextFetchTime = " + TimingUtil.format(nextFetchTime.get()));
    LOG.info("  aveCost(s)    = " + df.format(getAvarageTimeCost() / 1000));
    LOG.info("  fetchQueue    = " + fetchQueue.size());
    LOG.info("  fetchingQueue = " + fetchingQueue.size());
    LOG.info("  detached      = " + detached);

    for (int i = 0; i < Math.min(fetchQueue.size(), 20); i++) {
      final FetchItem it = fetchQueue.get(i);
      LOG.info("  " + (i + 1) + ". " + it.getUrl());
    }
  }

  private void addToFetchingQueue(FetchItem item, long now) {
    if (item == null) return;

    item.setPendingStartTime(now);
    fetchingQueue.put(item.getItemID(), item);
  }

  private void setEndTime(long endTime) {
    setEndTime(endTime, false);
  }

  private void setEndTime(long endTime, boolean asap) {
    if (!asap) {
      nextFetchTime.set(endTime + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
    }
    else {
      nextFetchTime.set(endTime);
    }
  }

  @Override
  public int compareTo(FetchItemQueue other) {
    return other.getPriority() - getPriority();
  }
}
