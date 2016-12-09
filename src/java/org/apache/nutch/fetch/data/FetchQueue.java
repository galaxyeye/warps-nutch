package org.apache.nutch.fetch.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.nutch.fetch.FetchMonitor;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * This class handles FetchItems which come from the same host ID (be it
 * a proto/hostname or proto/IP pair).
 *
 * It also keeps track of requests in progress and elapsed time between requests.
 */
public class FetchQueue implements Comparable<FetchQueue> {

  private static final Logger LOG = FetchMonitor.LOG;

  public enum Status { ACTIVITY, INACTIVITY, RETIRED }

  public static class Key implements Comparable<Key> {
    /** Queue priority, bigger item comes first **/
    private final int priority;
    /** Queue id */
    private final String queueId;

    public static Key create(int priority, String queueId) {
      return new Key(priority, queueId);
    }

    public Key(int priority, String queueId) {
      this.priority = priority;
      this.queueId = queueId;
    }

    public int getPriority() {
      return priority;
    }

    public String getQueueId() {
      return queueId;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Key)) return false;

      Key key = (Key)obj;
      return priority == key.getPriority() && queueId.equals(key.getQueueId());
    }

    @Override
    public int compareTo(Key key) {
      if (key == null) {
        return -1;
      }

      int p = getPriority(), p2 = key.getPriority();
      if (p == p2) {
        return queueId.compareTo(key.queueId);
      }

      return p - p2;
    }

    @Override
    public int hashCode() {
      return priority * 10000 + queueId.hashCode();
    }

    @Override
    public String toString() {
      return "<" + priority + ", " + queueId + ">";
    }
  }

  private Key key;

  private final long crawlDelay;    // millisecond
  private final long minCrawlDelay; // millisecond
  /** Max thread count for this queue */
  private final int maxThreads;
  /** If a task costs more then 10 milliseconds, it's a slow task */
  private final long slowTaskThreshold = 10;

  /** Hold all tasks ready to fetch */
  private final Queue<FetchTask> readyTasks = new LinkedList<>();
  /** Hold all tasks are fetching */
  private final Map<Integer, FetchTask> pendingTasks = new TreeMap<>();
  /**
   * Once timeout, the pending items should be put to the ready queue again.
   * time unit : milliseconds
   * */
  private final long pendingTimeout;
  private long nextFetchTime;

  /** Record timing cost of slow tasks */
  private final TreeSet<Long> costRecorder = new TreeSet<>();
  private int totalFinishedTasks = 1;
  private int totalFetchTime = 0;

  /**
   * Detach from the fetch queues, the queue does not accept any tasks, nor serve any requests,
   * but still hold pending tasks, waiting for finish
   * */
  private Status status = Status.ACTIVITY;

  public FetchQueue(int priority, String id, int maxThreads, long crawlDelay, long minCrawlDelay, long pendingTimeout) {
    this.key = new Key(priority, id);
    this.maxThreads = maxThreads;
    this.crawlDelay = crawlDelay;
    this.minCrawlDelay = minCrawlDelay;
    this.pendingTimeout = pendingTimeout;

    // ready to start
    setEndTime(System.currentTimeMillis() - crawlDelay);
  }

  public Key getKey() { return key; }

  public String getId() { return key.queueId; }

  public int getPriority() { return key.priority; }

  /** Produce a task to this queue. Retired queues do not accept any tasks */
  public void produce(FetchTask item) {
    if (item == null || status != Status.ACTIVITY) {
      return;
    }

    readyTasks.add(item);
  }

  /** Ask a task from this queue. Retired queues do not assign any tasks */
  public FetchTask consume(Set<String> exceptedHosts) {
    if (status != Status.ACTIVITY) {
      return null;
    }

    if (pendingTasks.size() >= maxThreads) {
      return null;
    }

    final long now = System.currentTimeMillis();
    if (nextFetchTime > now) {
      return null;
    }

    if (readyTasks.isEmpty()) {
      return null;
    }

    FetchTask fetchTask = null;
    try {
      fetchTask = readyTasks.remove();

      // Check whether it's unreachable
      if (exceptedHosts != null && !exceptedHosts.isEmpty()) {
        // TODO : check if we need to distinct URLUtil.HostGroupMode
        String domain = URLUtil.getDomainName(fetchTask.getUrl());
        // String host = URLUtil.getHostName(fetchTask.getUrl());
        while (exceptedHosts.contains(domain) && !readyTasks.isEmpty()) {
          fetchTask = readyTasks.remove();
          domain = URLUtil.getDomainName(fetchTask.getUrl());
        }
      }

      if (fetchTask != null) {
        hangUp(fetchTask, now);
      }
    } catch (final Exception e) {
      LOG.error("Failed to comsume fetch task. " + StringUtil.stringifyException(e));
    }

    return fetchTask;
  }

  /**
   * Note : We have set response time for each page, @see {HttpBase#getProtocolOutput}
   * */
  public void finish(FetchTask item, boolean asap) {
    pendingTasks.remove(item.getItemId());

    long endTime = System.currentTimeMillis();
    setEndTime(endTime, asap);

    // Record fetch time cost
    long fetchTime = endTime - item.getPendingStartTime();

    if (fetchTime > slowTaskThreshold) {
      // TODO : use a window-fixed queue (first in, first out)
      if (costRecorder.size() > 100) {
        costRecorder.clear();
      }

      costRecorder.add(fetchTime);
    }

    ++totalFinishedTasks;
    totalFetchTime += fetchTime;

    // TODO : why there is a 100 limitation?
    if (totalFinishedTasks > 100) {
      totalFinishedTasks = 0;
      totalFetchTime = 0;
    }
  }

  public void finish(int itemId, boolean asap) {
    FetchTask item = pendingTasks.get(itemId);
    if (item != null) {
      finish(item, asap);
    }
  }

  public FetchTask getPendingTask(int itemID) {
    return pendingTasks.get(itemID);
  }

  /**
   * Hang up the task and wait for completion. Move the fetch task to pending queue.
   * */
  private void hangUp(FetchTask fetchTask, long now) {
    if (fetchTask == null) return;

    fetchTask.setPendingStartTime(now);
    pendingTasks.put(fetchTask.getItemId(), fetchTask);
  }

  public boolean hasTasks() { return hasReadyTasks() || hasPendingTasks(); }

  public boolean hasReadyTasks() { return !readyTasks.isEmpty(); }

  public boolean hasPendingTasks() { return !pendingTasks.isEmpty(); }

  public boolean pendingTaskExist(int itemId) { return pendingTasks.containsKey(itemId); }

  public int readyCount() { return readyTasks.size(); }

  public int pendingCount() {
    return pendingTasks.size();
  }

  public int getFinishedTaskCount() { return totalFinishedTasks; }

  public int getSlowTaskCount() { return costRecorder.size(); }

  public boolean isSlow() { return isSlow(1); }

  public boolean isSlow(int costInSec) { return averageTimeCost() > costInSec; }

  /**
   * Avarage cost in seconds
   * */
  public double averageTimeCost() {
    return totalFetchTime / 1000.0 / totalFinishedTasks;
  }

  /**
   * Throught rate in seconds
   * */
  public double averageThroughputRate() {
    return totalFinishedTasks / (totalFetchTime / 1000.0);
  }

  public void disable() { this.status = Status.INACTIVITY; }

  public void retire() { this.status = Status.RETIRED; }

  public boolean isActive() { return this.status == Status.ACTIVITY; }

  public boolean isInactive() { return this.status == Status.INACTIVITY; }

  public boolean isRetired() { return this.status == Status.RETIRED; }

  public Status status() { return this.status; }

  /**
   * Retune the queue to avoid hung tasks, pending tasks are push to ready queue so they can be re-fetched
   *
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should the task should be restarted
   *
   * @param force If force is true, reload all pending fetch items immediately, otherwise, reload only exceeds pendingTimeout
   * */
  public void retune(boolean force) {
    long now = System.currentTimeMillis();

    final List<FetchTask> readyList = Lists.newArrayList();
    final Map<Integer, FetchTask> pendingList = Maps.newHashMap();

    pendingTasks.values().forEach(fetchTask -> {
      if (force || now - fetchTask.getPendingStartTime() > this.pendingTimeout) {
        readyList.add(fetchTask);
      }
      else {
        pendingList.put(fetchTask.getItemId(), fetchTask);
      }
    });

    // Another way to do it, it's much better if we have auto support
//    Predicate<FetchTask> predicate = fetchTask -> (force || now - fetchTask.getPendingStartTime() > this.pendingTimeout);
//    Stream<FetchTask> stream = pendingTasks.values().stream();
//    List<FetchTask> readyList = stream.filter(predicate).collect(Collectors.toList());
//    Map<Integer, FetchTask> pendingList = stream.filter(predicate.negate()).collect(Collectors.toMap());

    pendingTasks.clear();
    readyTasks.addAll(readyList);
    pendingTasks.putAll(pendingList);
  }

  public String getHost() {
    String host = null;

    try {
      // if (queueMode == "byDomain") {}
      host = URLUtil.getDomainName(getId());
    } catch (MalformedURLException ignored) {}

    return host == null ? "unknownHost" : host;
  }

  public String getCostReport() {
    return String.format("%1$40s -> aveTimeCost : %2$.2fs/p, avaThoPut : %3$.2fp/s",
        key.queueId, averageTimeCost(), averageThroughputRate());
  }

  public int clearReadyQueue() {
    int count = readyTasks.size();
    readyTasks.clear();
    return count;
  }

  public int clearPendingQueue() {
    int count = pendingTasks.size();
    pendingTasks.clear();
    return count;
  }

  public int clearPendingTasksIfFew(int threshold) {
    int count = pendingTasks.size();

    if (count > threshold) {
      return 0;
    }

    if (pendingTasks.isEmpty()) {
      return 0;
    }

    final String[] report = {"Clearing very slow pending items : "};
    final DecimalFormat df = new DecimalFormat("###0.##");
    long now = System.currentTimeMillis();

    pendingTasks.values().stream().limit(threshold).filter(Objects::nonNull).forEach(fetchItem -> {
      double elapsed = (now - fetchItem.getPendingStartTime()) / 1000.0;
      report[0] += String.format("\n%s -> pending for %ss", fetchItem.getUrl(), df.format(elapsed));
    });

    pendingTasks.clear();

    LOG.info(report[0]);

    return count;
  }

  public void dump() {
    final DecimalFormat df = new DecimalFormat("###0.##");

    LOG.info(Params.format(
        "className", getClass().getSimpleName(),
        "key", key,
        "maxThreads", maxThreads,
        "pendingTasks", pendingTasks.size(),
        "crawlDelay(s)", df.format(crawlDelay / 1000),
        "minCrawlDelay(s)", df.format(minCrawlDelay / 1000),
        "now", DateTimeUtil.now(),
        "nextFetchTime", DateTimeUtil.format(nextFetchTime),
        "aveTimeCost(s)", df.format(averageTimeCost()),
        "aveThoRate(s)", df.format(averageThroughputRate()),
        "readyTasks", readyTasks.size(),
        "pendingTasks", pendingTasks.size(),
        "status", status
    ));

    int i = 0;
    final int limit = 20;
    FetchTask fetchTask = readyTasks.poll();
    while (fetchTask != null && ++i <= limit) {
      LOG.info("  " + i + ". " + fetchTask.getUrl());
      fetchTask = readyTasks.poll();
    }
  }

  private void setEndTime(long endTime) {
    setEndTime(endTime, false);
  }

  private void setEndTime(long endTime, boolean asap) {
    if (!asap) {
      nextFetchTime = endTime + (maxThreads > 1 ? minCrawlDelay : crawlDelay);
    }
    else {
      nextFetchTime = endTime;
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof FetchQueue)) {
      return false;
    }

    return key.equals(((FetchQueue) other).key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public int compareTo(FetchQueue other) {
    if (other == null) return -1;

    return key.compareTo(other.key);
  }
}
