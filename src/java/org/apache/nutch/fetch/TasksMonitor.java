package org.apache.nutch.fetch;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.collections4.SortedBidiMap;
import org.apache.commons.collections4.bidimap.DualTreeBidiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetch.data.FetchQueue;
import org.apache.nutch.fetch.data.FetchQueues;
import org.apache.nutch.fetch.data.FetchTask;
import org.apache.nutch.host.HostDb;
import org.apache.nutch.mapreduce.NutchReporter;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nutch.fetch.TaskScheduler.QUEUE_REMAINDER_LIMIT;
import static org.apache.nutch.fetch.data.FetchQueues.*;
import static org.apache.nutch.metadata.Nutch.*;

/**
 * Keeps track of the all fetch items
 *
 * TODO : synchronize on workingQueues, not functions
 */
public class TasksMonitor {
  public static final Logger LOG = FetchMonitor.LOG;
  private final Logger REPORT_LOG = NutchReporter.LOG;

  private static final int THREAD_SEQUENCE_POS = "FetchThread-".length();

  private final Configuration conf;
  private final FetchQueues workingQueues = new FetchQueues();

  /**
   * Tracking time cost of each queue
   */
  private final SortedBidiMap<String, Double> queueTimeCosts = new DualTreeBidiMap<>();
  private final Multimap<String, String> queueServedThreads = TreeMultimap.create();
  private Set<String> unreachableHosts = new TreeSet<>();
  private Multiset<String> unreachableHostsTracker;

  private final AtomicInteger readyItemCount = new AtomicInteger(0);
  private final AtomicInteger pendingItemCount = new AtomicInteger(0);
  private final AtomicInteger finishedItemCount = new AtomicInteger(0);

  private final long crawlDelay;
  private final long minCrawlDelay;

  private String queueMode;
  private int maxQueueThreads; // TODO : max pending tasks
  private boolean useHostSettings = false;

  /**
   * Once timeout, the pending items should be put to the ready queue again.
   */
  private long pendingTimeout = 3 * 60 * 1000;

  private HostDb hostDb = null;

  private int nextQueuePosition = 0;

  private String reportSuffix;

  public TasksMonitor(Configuration conf) throws IOException {
    this.conf = conf;
    this.maxQueueThreads = conf.getInt(PARAM_FETCH_MAX_THREADS_PER_QUEUE, 1);
    FetchMode fetchMode = conf.getEnum(PARAM_FETCH_MODE, FetchMode.NATIVE);
    if (fetchMode == FetchMode.CROWDSOURCING) {
      this.maxQueueThreads = Integer.MAX_VALUE;
    }
    queueMode = conf.get(PARAM_FETCH_QUEUE_MODE, QUEUE_MODE_HOST);

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

    this.reportSuffix = conf.get(PARAM_NUTCH_JOB_NAME, "job-unknown-" + TimingUtil.now("MMdd.hhmm"));
    Path unreachableHostsPath = NutchConfiguration.getPath(conf,
        PARAM_NUTCH_UNREACHABLE_HOSTS_FILE, Paths.get(PATH_UNREACHABLE_HOSTS));
    unreachableHostsPath.toFile().createNewFile();
    Files.readAllLines(unreachableHostsPath).stream().forEach(unreachableHosts::add);

    DecimalFormat df = new DecimalFormat("###0.0#");
    LOG.info(Params.formatAsLine(
        "className", this.getClass().getSimpleName(),
        "maxThreadsPerQueue", maxQueueThreads,
        "queueMode", queueMode,
        "useHostSettings", useHostSettings,
        "crawlDelay(n)", df.format(crawlDelay / 60.0 / 1000.0),
        "minCrawlDelay(m)", df.format(minCrawlDelay / 60.0 / 1000.0),
        "pendingTimeout(m)", df.format(pendingTimeout / 60.0 / 1000.0),
        "unreachableHosts", unreachableHosts.size(),
        "unreachableHostsPath", unreachableHostsPath
    ));
  }

  public synchronized void produce(FetchTask item) {
    doProduce(item, FETCH_PRIORITY_DEFAULT);
  }

  public synchronized void produce(int jobID, String url, WebPage page) {
    final FetchTask it = FetchTask.create(jobID, url, page, queueMode);
    if (it != null) {
      // int priority = Integer.getInteger(TableUtil.getMetadata(page, META_FETCH_PRIORITY));
      doProduce(it, getPriority(page));
    }
  }

  private void doProduce(FetchTask item, int priority) {
    FetchQueue queue = getOrCreateFetchQueue(item.getQueueID(), priority);
    queue.produce(item);
    readyItemCount.incrementAndGet();
    queueTimeCosts.put(queue.getId(), 0.0);
  }

  private int getPriority(WebPage page) {
    return TableUtil.getPriority(page, FETCH_PRIORITY_DEFAULT);
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

    if (!queue.pendingTaskExist(itemId)) {
      // If the working queue is empty, the tasks might be abandoned by a plan
      if (!workingQueues.isEmpty() && isResourceUnreachable(queueId)) {
        LOG.warn("Attemp to finish unknown item: [{} - {}]", queueId, itemId);
      }

      return;
    }

    queue.finish(itemId, asap);
    pendingItemCount.decrementAndGet();
    finishedItemCount.incrementAndGet();

    queueTimeCosts.put(queueId, queue.avarageTimeCost());
    queueServedThreads.put(queueId, Thread.currentThread().getName().substring(THREAD_SEQUENCE_POS));
  }

  public synchronized void report() {
    dump(QUEUE_REMAINDER_LIMIT);

    reportUnreachableHosts();

    reportCost();

    reportServedThreads();
  }

  private void reportCost() {
    String report = "Top slow hosts : \n" + workingQueues.getCostReport();
    report += "\n";
    REPORT_LOG.info(report);
    NutchUtil.writeReport(report, "queue-costs-" + reportSuffix + ".txt", conf);
  }

  private void reportServedThreads() {
    StringBuilder report = new StringBuilder();
    queueServedThreads.keySet().forEach(queueId -> {
      String threads = "#" + StringUtils.join(queueServedThreads.get(queueId), ", #");
      String line = String.format("%1$40s -> %2$s\n", queueId, threads);
      report.append(line);
    });
    REPORT_LOG.info("Served threads : \n" + report);
    NutchUtil.writeReport("Served threads : \n" + report, "queue-served-threads-" + reportSuffix + ".txt", conf);
  }

  /**
   * Reload pending fetch items so that the items can be re-fetched
   * <p>
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should restart the task
   *
   * @param force reload all pending fetch items immediately
   */
  public synchronized void retune(boolean force) {
    LOG.info("Retune task queues ...");

    int readyCount = 0;
    int pendingCount = 0;

    for (FetchQueue queue : workingQueues.values()) {
      queue.retune(unreachableHosts, force);

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
    unreachableHostsTracker.add(host);

    if (unreachableHosts.contains(host)) {
      return;
    }

    // Only the exception occurs for unknownHostEventCount, it's really add to the black list
    final int unknownHostEventCount = 3;
    if (unreachableHostsTracker.count(host) > unknownHostEventCount) {
      LOG.info("Host unknown: " + host);
      final int maxUnreachableHosts = 10000;
      if (unreachableHosts.size() < maxUnreachableHosts) {
        unreachableHosts.add(host);
      }

      retune(true);
    }
  }

  public synchronized boolean isResourceUnreachable(String url) {
    return url != null && unreachableHosts.contains(getHost(url, "unknownHost"));
  }

  private String getHost(String url, String defaultHost) {
    String host = null;

    try {
      // if (queueMode == "byDomain") {}
      host = URLUtil.getDomainName(url);
    } catch (MalformedURLException ignored) {}

    return host == null ? defaultHost : host;
  }

  public synchronized void reportUnreachableHosts() {
    Set<String> unreachableDomainSet = new HashSet<>(this.unreachableHosts);
    if (unreachableDomainSet.isEmpty()) {
      return;
    }

    String report = "# Unreachable hosts : \n";

    if (!unreachableDomainSet.isEmpty()) {
      String hosts = StringUtils.join(unreachableDomainSet, '\n');
      report += hosts;
      report += "\n";
      REPORT_LOG.info(report);
    }

    Path unreachableHostsPath = Paths.get(PATH_UNREACHABLE_HOSTS);
    NutchUtil.writeReport(unreachableHostsPath, report);
  }

  public synchronized int clearSlowestQueue() {
    FetchQueue queue = getSlowestQueue();

    if (queue == null) {
      return 0;
    }

    workingQueues.detach(queue);

    clearPendingTasksIfFew(queue, 2);

    int deleted = clearReadyTasks(queue);

    DecimalFormat df = new DecimalFormat("0.##");

    LOG.warn("Detach slowest queue : " + queue.getId()
        + ", served tasks : " + queue.getFinishedTaskCount()
        + ", slow tasks : " + queue.getSlowTaskCount()
          + ", " + df.format(queue.avarageTimeCost()) + "s/p"
          + ", " + df.format(queue.avarageThroughputRate()) + "p/s"
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

  private int clearPendingTasksIfFew(FetchQueue queue, int limit) {
    int deleted = queue.clearPendingTasksIfFew(limit);
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
              host.getInt("q_mt", maxQueueThreads),
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
      queue = new FetchQueue(queueId, priority, maxQueueThreads, crawlDelay, minCrawlDelay, pendingTimeout);
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

      sb.append(String.format("%1$,4d.%2$,20.2f", i, entry.getKey()));
      sb.append(" <- ");
      sb.append(entry.getValue());
      sb.append("\n");
    }

    REPORT_LOG.info(sb.toString());

    NutchUtil.writeReport(sb.toString(), "queue-costs.txt-" + reportSuffix + ".txt", conf);
  }
}
