package org.apache.nutch.fetch;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeMultiset;
import org.apache.commons.collections4.SortedBidiMap;
import org.apache.commons.collections4.bidimap.DualTreeBidiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.common.Params;
import org.apache.nutch.fetch.data.FetchQueue;
import org.apache.nutch.fetch.data.FetchQueues;
import org.apache.nutch.fetch.data.FetchTask;
import org.apache.nutch.filter.CrawlFilters;
import org.apache.nutch.filter.PageCategory;
import org.apache.nutch.host.HostDb;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.Host;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.nutch.metadata.Nutch.*;
import static org.apache.nutch.util.URLUtil.EXAMPLE_HOST_NAME;

/**
 * Tasks Monitor
 */
public class TasksMonitor {
  public static final Logger LOG = FetchMonitor.LOG;
  public static final Logger REPORT_LOG = NutchMetrics.REPORT_LOG;

  private static final int THREAD_SEQUENCE_POS = "FetchThread-".length();

  private final FetchQueues fetchQueues = new FetchQueues();
  private int lastTaskPriority = Integer.MIN_VALUE;

  /**
   * Tracking time cost of each queue
   */
  private final SortedBidiMap<FetchQueue.Key, Double> queueTimeCosts = new DualTreeBidiMap<>();
  private final Multimap<String, String> queueServedThreads = TreeMultimap.create();
  private final Map<String, HostStat> availableHosts = new TreeMap<>();
  private final Set<String> unreachableHosts = new TreeSet<>();
  private final Multiset<String> unreachableHostsTracker = TreeMultiset.create();
  private final CrawlFilters crawlFilters;
  private final int maxUrlLength;

  private final AtomicInteger readyTaskCount = new AtomicInteger(0);
  private final AtomicInteger pendingTaskCount = new AtomicInteger(0);
  private final AtomicInteger finishedTaskCount = new AtomicInteger(0);

  private final Duration minCrawlDelay;
  private final Duration crawlDelay;

  private final URLUtil.HostGroupMode hostGroupMode;
  private final int maxQueueThreads; // TODO : max pending tasks
  private final boolean useHostSettings;

  private final NutchMetrics nutchMetrics;

  /**
   * Once timeout, the pending items should be put to the ready queue again.
   */
  private Duration queuePendingTimeout = Duration.ofMinutes(3);

  private HostDb hostDb = null;

  public TasksMonitor(Configuration conf) throws IOException {
    FetchMode fetchMode = conf.getEnum(PARAM_FETCH_MODE, FetchMode.NATIVE);
    maxQueueThreads = (fetchMode == FetchMode.CROWDSOURCING) ? Integer.MAX_VALUE : conf.getInt(PARAM_FETCH_MAX_THREADS_PER_QUEUE, 1);

    hostGroupMode = conf.getEnum(PARAM_FETCH_QUEUE_MODE, URLUtil.HostGroupMode.BY_HOST);
    useHostSettings = (hostGroupMode == URLUtil.HostGroupMode.BY_HOST) && conf.getBoolean("fetcher.queue.use.host.settings", false);

    if (useHostSettings) {
      LOG.info("Host specific queue settings enabled");
      hostDb = new HostDb(conf);
    }

    crawlFilters = CrawlFilters.create(conf);
    maxUrlLength = conf.getInt("nutch.url.filter.max.length", 1024);

    crawlDelay = Duration.ofMillis((long)(conf.getFloat("fetcher.server.delay", 1.0f) * 1000));
    minCrawlDelay = Duration.ofMillis((long)(conf.getFloat("fetcher.server.min.delay", 0.0f) * 1000));
    queuePendingTimeout = ConfigUtils.getDuration(conf, "fetcher.pending.timeout", Duration.ofMinutes(3));

    nutchMetrics = NutchMetrics.getInstance(conf);
    nutchMetrics.loadUnreachableHosts(unreachableHosts);

    DecimalFormat df = new DecimalFormat("###0.0#");
    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "maxThreadsPerQueue", maxQueueThreads,
        "hostGroupMode", hostGroupMode,
        "useHostSettings", useHostSettings,
        "crawlDelay(s)", df.format(crawlDelay.toMillis() / 1000.0),
        "minCrawlDelay(s)", df.format(minCrawlDelay.toMillis() / 1000.0),
        "queuePendingTimeout(m)", queuePendingTimeout.toMinutes(),
        "unreachableHosts", unreachableHosts.size(),
        "unreachableHostsPath", nutchMetrics.getUnreachableHostsPath()
    ));
  }

  public final URLUtil.HostGroupMode getHostGroupMode() {
    return hostGroupMode;
  }

  public synchronized void produce(FetchTask item) {
    doProduce(item);
  }

  public synchronized void produce(int jobID, String url, WebPage page) {
    int priority = page.getFetchPriority();
    FetchTask task = FetchTask.create(jobID, priority, url, page, hostGroupMode);

    if (task != null) {
      produce(task);
    }
  }

  private void doProduce(FetchTask item) {
    int priority = item.getPriority();
    String queueId = item.getQueueId();
    FetchQueue queue = fetchQueues.find(FetchQueue.Key.create(priority, queueId));

    if (queue == null) {
      queue = createFetchQueue(priority, queueId);
      fetchQueues.add(queue);
    }
    queue.produce(item);

    readyTaskCount.incrementAndGet();
    queueTimeCosts.put(queue.getKey(), 0.0);
  }

  /**
   * Find out the FetchQueue with top priority,
   * wait for all pending tasks with higher priority are finished
   * */
  public synchronized FetchTask consume() {
    if (fetchQueues.isEmpty()) {
      return null;
    }

    final int nextPriority = fetchQueues.peek().getPriority();
    final boolean priorityChanged = nextPriority < lastTaskPriority;
    if (priorityChanged && fetchQueues.hasPriorPendingTasks(nextPriority)) {
      // Waiting for all pending tasks with higher priority to be finished
      return null;
    }

    if (priorityChanged) {
      LOG.info("Fetch priority changed : " + lastTaskPriority + " -> " + nextPriority);
    }

    FetchQueue queue = null;
    while (queue == null && !fetchQueues.isEmpty()) {
      queue = checkFetchQueue(fetchQueues.peek());
    }

    return consumeUnchecked(queue);
  }

  public synchronized FetchTask consume(int priority, String queueId) {
    FetchQueue queue = fetchQueues.find(FetchQueue.Key.create(priority, queueId));
    return consumeUnchecked(checkFetchQueue(queue));
  }

  public synchronized FetchTask consume(FetchQueue queue) { return consumeUnchecked(checkFetchQueue(queue)); }

  @Contract("null -> null")
  private FetchTask consumeUnchecked(FetchQueue queue) {
    if (queue == null) {
      return null;
    }

    FetchTask item = queue.consume(unreachableHosts);

    if (item != null) {
      readyTaskCount.decrementAndGet();
      pendingTaskCount.incrementAndGet();
      lastTaskPriority = queue.getPriority();
    }

    return item;
  }

  @Contract("null -> null")
  private FetchQueue checkFetchQueue(FetchQueue queue) {
    if (queue == null) {
      return null;
    }

    boolean changed = maintainFetchQueueLifeTime(queue);

    return changed ? null : queue;
  }

  /** Maintain queue life time, return true if the life time status is changed, false otherwise */
  @Contract("null -> false")
  private boolean maintainFetchQueueLifeTime(FetchQueue queue) {
    if (queue == null) {
      return false;
    }

    if (queue.isActive() && !queue.hasReadyTasks()) {
      // There are still some pending tasks, the queue do not serve new requests, but should finish pending tasks
      fetchQueues.disable(queue);
//      LOG.debug("FetchQueue " + queue.getKey() + " is disabled");
      LOG.debug("FetchQueue " + queue.getKey() + " is disabled, task stat : "
          + "ready : " + queue.readyCount()
          + ", pending : " + queue.pendingCount()
          + ", finished : " + queue.finishedCount());
      return true;
    }
    else if (queue.isInactive() && !queue.hasTasks()) {
      // All tasks are finished, including pending tasks, we can remove the queue from the queues safely
      retire(queue);
//      LOG.debug("FetchQueue " + queue.getKey() + " is retired");
      LOG.debug("FetchQueue " + queue.getKey() + " is retired, task stat : "
          + "ready : " + queue.readyCount()
          + ", pending : " + queue.pendingCount()
          + ", finished : " + queue.finishedCount());
      return true;
    }

    return false;
  }

  public synchronized void finish(int priority, String queueId, int itemId, boolean asap) {
    doFinish(priority, queueId, itemId, asap);
  }

  public synchronized void finish(FetchTask item) {
    doFinish(item.getPriority(), item.getQueueId(), item.getItemId(), false);
  }

  public synchronized void finishAsap(FetchTask item) {
    finish(item.getPriority(), item.getQueueId(), item.getItemId(), true);
  }

  private void doFinish(int priority, String queueId, int itemId, boolean asap) {
    FetchQueue queue = fetchQueues.findExtend(FetchQueue.Key.create(priority, queueId));

    if (queue == null) {
      LOG.warn("Attemp to finish item from unknown queue: <{}, {}>", priority, queueId);
      return;
    }

    if (!queue.pendingTaskExist(itemId)) {
      if (!fetchQueues.isEmpty() && isResourceUnreachable(queueId)) {
        LOG.warn("Attemp to finish unknown item: <{}, {}, {}>", priority, queueId, itemId);
      }

      return;
    }

    queue.finish(itemId, asap);

    pendingTaskCount.decrementAndGet();
    finishedTaskCount.incrementAndGet();

    queueTimeCosts.put(FetchQueue.Key.create(priority, queueId), queue.averageRecentTimeCost());
    queueServedThreads.put(queueId, Thread.currentThread().getName().substring(THREAD_SEQUENCE_POS));
  }

  private void retire(FetchQueue queue) {
    queue.retire();
    fetchQueues.remove(queue);
  }

  public synchronized void report() {
    dump(FETCH_TASK_REMAINDER_NUMBER);

    reportAndLogUnreachableHosts();

    reportAndLogAvailableHosts();

    reportCost();

    reportServedThreads();
  }

  /**
   * Reload pending fetch items so that the items can be re-fetched
   * <p>
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should restart the task
   *
   * @param force reload all pending fetch items immediately
   * */
  public synchronized void retune(boolean force) {
    List<FetchQueue> unreachableQueues = fetchQueues.stream()
        .filter(queue -> unreachableHosts.contains(queue.getHost())).collect(toList());
    unreachableQueues.forEach(this::retire);

    String unreachableQueueIds = unreachableQueues.stream().map(FetchQueue::getId).collect(joining(", "));
    if (!unreachableQueueIds.isEmpty()) {
      LOG.info("Retired queues for host unreachable : " + unreachableQueueIds);
    }

    fetchQueues.forEach(queue -> queue.retune(force));

    calculateTaskCounter();
  }

  /** Get a pending task, the task can be in working queues or in detached queues */
  public synchronized FetchTask findPendingTask(int priority, String queueId, int itemID) {
    FetchQueue queue = fetchQueues.findExtend(FetchQueue.Key.create(priority, queueId));
    return queue != null ? queue.getPendingTask(itemID) : null;
  }

  public synchronized void dump(int limit) {
    fetchQueues.dump(limit);
    calculateTaskCounter();
  }

  /**
   * Available hosts statistics
   * */
  public synchronized void statHost(String url, WebPage page) {
    if (url == null || url.isEmpty()) {
      return;
    }

    if (!page.hasMark(Mark.PARSE)) {
      LOG.warn("Can not stat page without parsing, url : " + url);
      return;
    }

    String host = URLUtil.getHost(url, EXAMPLE_HOST_NAME, getHostGroupMode());
    if (host == null || host.isEmpty()) {
      return;
    }

    HostStat hostStat = availableHosts.get(host);
    if (hostStat == null) {
      hostStat = new HostStat(host);
    }

    ++hostStat.urls;

    // PageCategory pageCategory = CrawlFilter.sniffPageCategory(url, page);
    PageCategory pageCategory = page.getPageCategory();
    if (pageCategory.isIndex()) {
      ++hostStat.indexUrls;
    } else if (pageCategory.isDetail()) {
      ++hostStat.detailUrls;
    } else if (pageCategory.isMedia()) {
      ++hostStat.mediaUrls;
    } else if (pageCategory.isSearch()) {
      ++hostStat.searchUrls;
    } else if (pageCategory.isBBS()) {
      ++hostStat.bbsUrls;
    } else if (pageCategory.isTieBa()) {
      ++hostStat.tiebaUrls;
    } else if (pageCategory.isBlog()) {
      ++hostStat.blogUrls;
    } else if (pageCategory.isUnknown()) {
      ++hostStat.unknownUrls;
    }

    int depth = page.getDepth();
    if (depth == 1) {
      ++hostStat.urlsFromSeed;
    }

    if (url.length() > maxUrlLength) {
      ++hostStat.urlsTooLong;
      nutchMetrics.debugLongUrls(url);
    }

    availableHosts.put(host, hostStat);
  }

  public synchronized void statUnreachableHost(String url) {
    if (url == null || url.isEmpty()) {
      return;
    }

    String host = URLUtil.getHost(url, EXAMPLE_HOST_NAME, getHostGroupMode());
    if (host == null || host.isEmpty()) {
      return;
    }

    unreachableHostsTracker.add(host);

    if (unreachableHosts.contains(host)) {
      return;
    }

    // Only the exception occurs for unknownHostEventCount, it's really add to the black list
    final int unknownHostEventCount = 10;
    if (unreachableHostsTracker.count(host) > unknownHostEventCount) {
      LOG.info("Host unknown: " + host);
      unreachableHosts.add(host);
      retune(true);
    }
  }

  public synchronized boolean isResourceUnreachable(String url) {
    return url != null && unreachableHosts.contains(URLUtil.getHost(url, EXAMPLE_HOST_NAME, hostGroupMode));
  }

  public synchronized int clearSlowestQueue() {
    FetchQueue queue = getSlowestQueue();

    if (queue == null) {
      return 0;
    }

    // slowest queues should retire as soon as possible
    retire(queue);

    final int minPendingSlowTasks = 2;
    clearPendingTasksIfFew(queue, minPendingSlowTasks);

    int deleted = clearReadyTasks(queue);

    final DecimalFormat df = new DecimalFormat("0.##");

    LOG.warn("Retire slowest queue : " + queue.getId()
        + ", task stat : \n"
        + "\t\tready : " + queue.readyCount()
        + ", pending : " + queue.pendingCount()
        + ", finished : " + queue.finishedCount()
        + ", slow : " + queue.slowTaskCount()
        + ", " + df.format(queue.averageTimeCost()) + "s/p"
        + ", " + df.format(queue.averageThoRate()) + "p/s"
        + ", deleted : " + deleted
    );

    return deleted;
  }

  public synchronized void cleanup() {
    fetchQueues.clear();
    readyTaskCount.set(0);
  }

  public synchronized int clearReadyTasks() {
    int count = 0;

    Map<Double, String> costRecorder = new TreeMap<>(Comparator.reverseOrder());
    for (FetchQueue queue : fetchQueues) {
      costRecorder.put(queue.averageRecentTimeCost(), queue.getId());

      if (queue.readyCount() == 0) {
        continue;
      }

      count += clearReadyTasks(queue);
    }

    reportCost(costRecorder);

    return count;
  }

  private int clearPendingTasksIfFew(FetchQueue queue, int limit) {
    int deleted = queue.clearPendingTasksIfFew(limit);
    pendingTaskCount.addAndGet(-deleted);
    return deleted;
  }

  private int clearReadyTasks(FetchQueue queue) {
    int deleted = queue.clearReadyQueue();

    readyTaskCount.addAndGet(-deleted);
    if (readyTaskCount.get() <= 0 && fetchQueues.size() == 0) {
      readyTaskCount.set(0);
    }

    return deleted;
  }

  public synchronized int getQueueCount() {
    return fetchQueues.size();
  }

  public int taskCount() { return readyTaskCount.get() + pendingTaskCount(); }

  public int readyTaskCount() { return readyTaskCount.get(); }

  public int pendingTaskCount() { return pendingTaskCount.get(); }

  public int getFinishedTaskCount() {return finishedTaskCount.get(); }

  private void calculateTaskCounter() {
    int[] counter = {0, 0};
    fetchQueues.forEach(queue -> {
      counter[0] += queue.readyCount();
      counter[1] += queue.pendingCount();
    });
    readyTaskCount.set(counter[0]);
    pendingTaskCount.set(counter[1]);
  }

  private FetchQueue getSlowestQueue() {
    FetchQueue queue = null;

    while (!fetchQueues.isEmpty() && queue == null) {
      double maxCost = queueTimeCosts.inverseBidiMap().lastKey();
      FetchQueue.Key slowestQueueKey = queueTimeCosts.inverseBidiMap().get(maxCost);
      queueTimeCosts.remove(slowestQueueKey);

      queue = fetchQueues.find(slowestQueueKey);
    }

    return queue;
  }

  private FetchQueue createFetchQueue(int priority, String queueId) {
    FetchQueue queue = null;

    // Create a new queue
    if (useHostSettings) {
      // Use host specific queue settings (if defined in the host table)
      try {
        String hostname = queueId.substring(queueId.indexOf("://") + 3);
        Host host = hostDb.getByHostName(hostname);
        if (host != null) {
          queue = new FetchQueue(
              priority,
              queueId,
              host.getInt("q_mt", maxQueueThreads),
              Duration.ofMillis(host.getLong("q_cd", crawlDelay.toMillis())),
              Duration.ofMillis(host.getLong("q_mcd", minCrawlDelay.toMillis())),
              queuePendingTimeout);
        }
      } catch (IOException e) {
        LOG.error("Error while trying to access host settings", e);
      }
    }

    if (queue == null) {
      // Use queue defaults
      queue = new FetchQueue(priority, queueId, maxQueueThreads, crawlDelay, minCrawlDelay, queuePendingTimeout);
    }

    return queue;
  }

  private void reportServedThreads() {
    StringBuilder report = new StringBuilder();
    queueServedThreads.keySet().stream()
        .map(TableUtil::reverseHost).sorted().map(TableUtil::unreverseHost)
        .forEach(queueId -> {
      String threads = "#" + StringUtils.join(queueServedThreads.get(queueId), ", #");
      String line = String.format("%1$40s -> %2$s\n", queueId, threads);
      report.append(line);
    });

    REPORT_LOG.info("Served threads : \n" + report);
  }

  private void reportAndLogUnreachableHosts() {
    Set<String> unreachableDomainSet = new TreeSet<>(this.unreachableHosts);
    if (unreachableDomainSet.isEmpty()) {
      return;
    }

    String report = "";
    String hosts = unreachableDomainSet.stream()
        .map(StringUtil::reverse).sorted().map(StringUtil::reverse)
        .collect(joining("\n"));
    report += hosts;
    report += "\n";
    REPORT_LOG.info("# Unreachable hosts : \n" + report);

    try {
      Path unreachableHostsPath = nutchMetrics.getUnreachableHostsPath();
      nutchMetrics.loadUnreachableHosts(unreachableDomainSet);
      Files.write(unreachableHostsPath, report.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    } catch (IOException e) {
      LOG.error(e.toString());
    }
  }

  private void reportAndLogAvailableHosts() {
    String report = "# Total " + availableHosts.size() + " available hosts";
    report += "\n";

    String hostsReport = availableHosts.values().stream().sorted()
        .map(hostInfo -> String.format("%40s -> %-15s %-15s %-15s %-15s %-15s %-15s %-15s %-15s %-15s",
            hostInfo.hostName,
            "total : " + hostInfo.urls,
            "index : " + hostInfo.indexUrls,
            "detail : " + hostInfo.detailUrls,
            "search : " + hostInfo.searchUrls,
            "media : " + hostInfo.mediaUrls,
            "bbs : " + hostInfo.bbsUrls,
            "tieba : " + hostInfo.tiebaUrls,
            "blog : " + hostInfo.blogUrls,
            "long : " + hostInfo.urlsTooLong))
        .collect(joining("\n"));

    report += hostsReport;
    report += "\n";

    REPORT_LOG.info(report);
  }

  private void reportCost(Map<Double, String> costRecorder) {
    StringBuilder sb = new StringBuilder();

    sb.append(String.format("\n%s\n", "---------------Queue Cost Report--------------"));
    sb.append(String.format("%25s %s\n", "Ava Time(s)", "Queue Id"));
    final int[] i = {0};
    costRecorder.entrySet().stream().limit(100).forEach(entry -> {
      sb.append(String.format("%1$,4d.%2$,20.2f", ++i[0], entry.getKey()));
      sb.append(" <- ");
      sb.append(entry.getValue());
      sb.append("\n");
    });

    REPORT_LOG.info(sb.toString());
  }

  private void reportCost() {
    String report = "Top slow hosts : \n" + fetchQueues.getCostReport();
    report += "\n";
    REPORT_LOG.info(report);
  }
}
