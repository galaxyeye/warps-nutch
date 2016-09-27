package org.apache.nutch.fetch.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TimingUtil;
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
  public static final Logger LOG = FetchQueues.LOG;

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
  private final Queue<FetchTask> readyTasks = new LinkedList<>();
  /** Hold all tasks are fetching */
  private final Map<Integer, FetchTask> pendingTasks = new TreeMap<>();
  // private final Set<FetchTask> pendingTasks = new TreeSet<>();
  /**
   * Once timeout, the pending items should be put to the ready queue again.
   * time unit : seconds
   * */
  private final long pendingTimeout;
  private long nextFetchTime;

  /** Record timing cost of slow tasks */
  private final Set<Long> costRecorder = Sets.newTreeSet();
  private int totalFinishedTasks = 1;
  private int totalFetchTime = 0;

  /**
   * Detached from the system, the queue does not accept any tasks, nor serve any requests,
   * but still hold pending tasks
   * */
  private boolean detached = false;

  public FetchQueue(String id, int priority, int maxThreads, long crawlDelay, long minCrawlDelay, long pendingTimeout) {
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

  public void produce(FetchTask item) {
    if (item == null || detached) {
      return;
    }

    readyTasks.add(item);
  }

  public FetchTask consume(Set<String> exceptedHosts) {
    if (detached) {
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
        // if (queueMode.equals(byDomain))
        String domain = URLUtil.getDomainName(fetchTask.getUrl());
        while (exceptedHosts.contains(domain) && !readyTasks.isEmpty()) {
          fetchTask = readyTasks.remove();
          domain = URLUtil.getDomainName(fetchTask.getUrl());
        }
      }

      if (fetchTask != null) {
        hang(fetchTask, now);
      }
    } catch (final Exception e) {
      LOG.error("Failed to comsume fetch task, {}", e);
    }

    return fetchTask;
  }

  public FetchTask getPendingTask(int itemID) {
    return pendingTasks.get(itemID);
  }

  private void hang(FetchTask fetchTask, long now) {
    if (fetchTask == null) return;

    fetchTask.setPendingStartTime(now);
    pendingTasks.put(fetchTask.getItemID(), fetchTask);
  }

  /**
   *
   * Note : We have set response time for each page, @see {HttpBase#getProtocolOutput}
   * */
  public void finish(FetchTask item, boolean asap) {
    pendingTasks.remove(item.getItemID());

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

    ++totalFinishedTasks;
    totalFetchTime += fetchTime;
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

  public boolean hasTasks() { return !readyTasks.isEmpty(); }

  public boolean pendingTaskExist(int itemId) {
    return pendingTasks.containsKey(itemId);
  }

  public int readyCount() { return readyTasks.size(); }

  public int pendingCount() {
    return pendingTasks.size();
  }

  public int getFinishedTaskCount() { return totalFinishedTasks; }

  public int getSlowTaskCount() { return costRecorder.size(); }

  public boolean isSlow() { return isSlow(1); }

  public boolean isSlow(int costInSec) { return avarageTimeCost() > costInSec; }

  /**
   * Avarage cost in seconds
   * */
  public double avarageTimeCost() {
    return totalFetchTime / 1000.0 / totalFinishedTasks;
  }

  /**
   * Throught rate in seconds
   * */
  public double avarageThroughputRate() {
    return totalFinishedTasks / (totalFetchTime / 1000.0);
  }

  public void detach() { this.detached = true; }

  public boolean isDetached() { return this.detached; }

  /**
   * Retune the queue to avoid hung tasks, pending tasks are push to ready queue so they can be re-fetched
   *
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should the task should be restarted
   *
   * @param force If force is true, reload all pending fetch items immediately, otherwise, reload only exceeds pendingTimeout
   * */
  public void retune(Set<String> unreachableHosts, boolean force) {
    if (unreachableHosts.contains(getHost())) {
      LOG.info("Remove queue for host unreachable : " + getId());
      readyTasks.clear();
      pendingTasks.clear();
      detach();

      return;
    }

    long now = System.currentTimeMillis();

    final List<FetchTask> readyList = Lists.newArrayList();
    final Map<Integer, FetchTask> pendingList = Maps.newHashMap();

    pendingTasks.values().forEach(fetchTask -> {
      if (force || now - fetchTask.getPendingStartTime() > this.pendingTimeout) {
        readyList.add(fetchTask);
      }
      else {
        pendingList.put(fetchTask.getItemID(), fetchTask);
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
    return String.format("%1$40s -> avaTimeCost : %2$.2fs/p, avaThoPut : %3$.2fp/s",
        id, avarageTimeCost(), avarageThroughputRate());
  }

  public int clearReadyQueue() {
    int count = readyTasks.size();
    readyTasks.clear();
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
        "id", id,
        "maxThreads", maxThreads,
        "pendingTasks", pendingTasks.size(),
        "crawlDelay(s)", df.format(crawlDelay / 1000),
        "minCrawlDelay(s)", df.format(minCrawlDelay / 1000),
        "now", TimingUtil.now(),
        "nextFetchTime", TimingUtil.format(nextFetchTime),
        "aveTimeCost(s)", df.format(avarageTimeCost()),
        "aveThoRate(s)", df.format(avarageThroughputRate()),
        "readyTasks", readyTasks.size(),
        "pendingTasks", pendingTasks.size(),
        "detached", detached
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
  public int compareTo(FetchQueue other) {
    return other.getPriority() - getPriority();
  }
}
