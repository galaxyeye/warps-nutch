package org.apache.nutch.fetch.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.nutch.fetch.FetchMonitor;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.common.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * This class handles FetchItems which come from the same host ID (be it
 * a proto/hostname or proto/IP pair).
 *
 * It also keeps track of requests in progress and elapsed time between requests.
 */
public class FetchQueue implements Comparable<FetchQueue> {

  private static final Logger LOG = FetchMonitor.LOG;

  private final int RECENT_TASKS_COUNT_LIMIT = 100;

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

  private final Duration crawlDelay;
  private final Duration minCrawlDelay;

  /** Max thread count for this queue */
  private final int maxThreads;
  /** Hold all tasks ready to fetch */
  private final Queue<FetchTask> readyTasks = new LinkedList<>();
  /** Hold all tasks are fetching */
  private final Map<Integer, FetchTask> pendingTasks = new TreeMap<>();
  /** Once timeout, the pending items should be put to the ready queue again */
  private final Duration pendingTimeout;
  /** Next fetch time */
  private Instant nextFetchTime;

  /** If a task costs more then 0.5 seconds, it's a slow task */
  private final Duration slowTaskThreshold = Duration.ofMillis(500);
  /** Record timing cost of slow tasks */
  private final CircularFifoQueue<Duration> slowTasksRecorder = new CircularFifoQueue<>(RECENT_TASKS_COUNT_LIMIT);
  private int recentFinishedTasks = 1;
  private long recentFetchMillis = 1;
  private int totalFinishedTasks = 1;
  private long totalFetchMillis = 1;

  /**
   * If a fetch queue is inactive, the queue does not accept any tasks, nor serve any requests,
   * but still hold pending tasks, waiting to finish
   * */
  private Status status = Status.ACTIVITY;

  public FetchQueue(int priority, String id, int maxThreads, Duration crawlDelay, Duration minCrawlDelay, Duration pendingTimeout) {
    this.key = new Key(priority, id);
    this.maxThreads = maxThreads;
    this.crawlDelay = crawlDelay;
    this.minCrawlDelay = minCrawlDelay;
    this.pendingTimeout = pendingTimeout;
    this.nextFetchTime = Instant.now();
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

    Instant now = Instant.now();
    if (now.isBefore(nextFetchTime)) {
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
  public boolean finish(FetchTask fetchTask, boolean asap) {
    int itemId = fetchTask.getItemId();
    fetchTask = pendingTasks.remove(itemId);
    if (fetchTask == null) {
      LOG.warn("Failed to remove FetchTask : " + itemId);
      return false;
    }

    Instant finishTime = Instant.now();
    setNextFetchTime(finishTime, asap);

    Duration timeCost = Duration.between(fetchTask.getPendingStart(), finishTime);
    if (timeCost.compareTo(slowTaskThreshold) > 0) {
      slowTasksRecorder.add(timeCost);
    }

    ++recentFinishedTasks;
    recentFetchMillis += timeCost.toMillis();
    if (recentFinishedTasks > RECENT_TASKS_COUNT_LIMIT) {
      recentFinishedTasks = 1;
      recentFetchMillis = 1;
    }

    ++totalFinishedTasks;
    totalFetchMillis += timeCost.toMillis();

    return true;
  }

  public boolean finish(int itemId, boolean asap) {
    FetchTask item = pendingTasks.get(itemId);
    if (item != null) {
      return finish(item, asap);
    }

    return false;
  }

  public FetchTask getPendingTask(int itemID) { return pendingTasks.get(itemID); }

  /**
   * Hang up the task and wait for completion. Move the fetch task to pending queue.
   * */
  private void hangUp(FetchTask fetchTask, Instant now) {
    if (fetchTask == null) {
      return;
    }

    fetchTask.setPendingStart(now);
    pendingTasks.put(fetchTask.getItemId(), fetchTask);
  }

  public boolean hasTasks() { return hasReadyTasks() || hasPendingTasks(); }

  public boolean hasReadyTasks() { return !readyTasks.isEmpty(); }

  public boolean hasPendingTasks() { return !pendingTasks.isEmpty(); }

  public boolean pendingTaskExist(int itemId) { return pendingTasks.containsKey(itemId); }

  public int readyCount() { return readyTasks.size(); }

  public int pendingCount() { return pendingTasks.size(); }

  public int finishedCount() { return totalFinishedTasks; }

  public int slowTaskCount() { return slowTasksRecorder.size(); }

  public boolean isSlow() { return isSlow(Duration.ofSeconds(1)); }

  public boolean isSlow(Duration threshold) { return averageRecentTimeCost() > threshold.getSeconds(); }

  /**
   * Average cost in seconds
   * */
  public double averageTimeCost() { return totalFetchMillis / 1000.0 / totalFinishedTasks; }
  public double averageRecentTimeCost() { return recentFetchMillis / 1000.0 / recentFinishedTasks; }

  /**
   * Throughput rate in seconds
   * */
  public double averageThoRate() { return totalFinishedTasks / (totalFetchMillis / 1000.0); }
  public double averageRecentThoRate() { return recentFinishedTasks / (recentFetchMillis / 1000.0); }

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
    Instant now = Instant.now();

    final List<FetchTask> readyList = Lists.newArrayList();
    final Map<Integer, FetchTask> pendingList = Maps.newHashMap();

    pendingTasks.values().forEach(fetchTask -> {
      if (force || fetchTask.getPendingStart().plus(pendingTimeout).isBefore(now)) {
        readyList.add(fetchTask);
      }
      else {
        pendingList.put(fetchTask.getItemId(), fetchTask);
      }
    });

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
    return String.format("%1$40s -> aveTimeCost : %2$.2fs/p, avaThoRate : %3$.2fp/s",
        key.queueId, averageTimeCost(), averageThoRate());
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
    final Instant now = Instant.now();

    pendingTasks.values().stream().limit(threshold).filter(Objects::nonNull).forEach(fetchItem -> {
//      double elapsed = (now - fetchItem.getPendingStart()) / 1000.0;
      Duration elapsed = Duration.between(fetchItem.getPendingStart(), now);
      report[0] += String.format("\n%s -> pending for %ss", fetchItem.getUrl(), df.format(elapsed.toMillis() / 1000.0));
    });

    pendingTasks.clear();

    LOG.info(report[0]);

    return count;
  }

  public void dump() {
    final DecimalFormat df = new DecimalFormat("###0.##");

    LOG.info(Params.formatAsLine(
        "className", getClass().getSimpleName(),
        "status", status,
        "key", key,
        "maxThreads", maxThreads,
        "pendingTasks", pendingTasks.size(),
        "crawlDelay(s)", df.format(crawlDelay.getSeconds()),
        "minCrawlDelay(s)", df.format(minCrawlDelay.getSeconds()),
        "now", DateTimeUtil.now(),
        "nextFetchTime", DateTimeUtil.format(nextFetchTime),
        "aveTimeCost(s)", df.format(averageTimeCost()),
        "aveThoRate(s)", df.format(averageThoRate()),
        "readyTasks", readyCount(),
        "pendingTasks", pendingCount(),
        "finsihedTasks", finishedCount()
    ));

    int i = 0;
    final int limit = 20;
    String report = "\nDrop the following tasks : ";
    FetchTask fetchTask = readyTasks.poll();
    while (fetchTask != null && ++i <= limit) {
      report += "  " + i + ". " + fetchTask.getUrl() + "\t";
      fetchTask = readyTasks.poll();
    }
    LOG.info(report);
  }

  private void setNextFetchTime(Instant finishTime, boolean asap) {
    if (!asap) {
      nextFetchTime = finishTime.plus(maxThreads > 1 ? minCrawlDelay : crawlDelay);
    }
    else {
      nextFetchTime = finishTime;
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
