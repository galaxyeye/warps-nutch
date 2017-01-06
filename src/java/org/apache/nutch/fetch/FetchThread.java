package org.apache.nutch.fetch;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetch.data.FetchTask;
import org.apache.nutch.fetch.service.FetchResult;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This class picks items from queues and fetches the pages.
 * */
public class FetchThread extends Thread implements Comparable<FetchThread> {

  enum FetchStatus { Success, Failed}

  private class FetchItem {
    FetchItem(FetchTask task, FetchResult result) {
      this.task = task;
      this.result = result;
    }

    FetchResult result;
    FetchTask task;
  }

  private final Logger LOG = FetchMonitor.LOG;
  private final Logger REPORT_LOG = NutchMetrics.REPORT_LOG;

  private static AtomicInteger instanceSequence = new AtomicInteger(0);

  private final Configuration conf;
  private final int id;

  private final ProtocolFactory protocolFactory;
  private final TaskScheduler taskScheduler;
  /**
   * Native, Crowdsourcing, Proxy
   * */
  private final FetchMode fetchMode;
  private boolean debugContent = false;
  private String reprUrl;

  /** Fix the thread to a specified queue as possible as we can */
  private int currPriority = -1;
  private String currQueueId = null;
  private AtomicBoolean halted = new AtomicBoolean(false);
  private Set<String> servedHosts = new TreeSet<>();
  private int taskCount = 0;

  public FetchThread(TaskScheduler taskScheduler, Configuration conf) {
    this.conf = conf;

    this.taskScheduler = taskScheduler;

    this.id = instanceSequence.incrementAndGet();

    this.setDaemon(true);
    this.setName(getClass().getSimpleName() + "-" + id);

    this.protocolFactory = new ProtocolFactory(conf);
    this.fetchMode = conf.getEnum("fetcher.fetch.mode", FetchMode.NATIVE);
    this.debugContent = conf.getBoolean("fetcher.fetch.thread.debug.content", false);
  }

  public String reprUrl() { return reprUrl; }

  public void halt() { halted.set(true); }

  public void exitAndJoin() {
    halted.set(true);
    try {
      join();
    } catch (InterruptedException e) {
      LOG.error(e.toString());
    }
  }

  public boolean isHalted() { return halted.get(); }

  @Override
  public void run() {
    taskScheduler.registerFetchThread(this);

    FetchItem fetchItem = null;

    try {
      while (!taskScheduler.isMissionComplete() && !isHalted()) {
        fetchItem = schedule();

        if (fetchItem.task == null) {
          sleepAndRecord();
          continue;
        }

        fetchOne(fetchItem);

        ++taskCount;
      } // while
    } catch (final Throwable e) {
      LOG.error("Unexpected throwable : " + StringUtil.stringifyException(e));
    } finally {
      if (fetchItem != null && fetchItem.task != null) {
        taskScheduler.finishUnchecked(fetchItem.task);
      }

      taskScheduler.unregisterFetchThread(this);

      LOG.info("Thread #{} finished, {} active threads", getId(), taskScheduler.getActiveFetchThreadCount());
    }
  }

  public void report() {
    String report = String.format("Thread #%d served %d tasks for %d hosts : \n", getId(), taskCount, servedHosts.size());
    report += "\n";

    String availableHosts = servedHosts.stream()
        .map(TableUtil::reverseHost).sorted().map(TableUtil::unreverseHost)
        .map(host -> String.format("%1$40s", host))
        .collect(Collectors.joining("\n"));

    report += availableHosts;
    report += "\n";

    REPORT_LOG.info(report);
  }

  private void sleepAndRecord() {
    taskScheduler.registerIdleThread(this);

    try {
      Thread.sleep(2000);
    } catch (final Exception ignored) {}

    taskScheduler.unregisterIdleThread(this);
  }

  private FetchItem schedule() {
    FetchResult fetchResult = null;
    FetchTask fetchTask = null;

    if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
      fetchResult = taskScheduler.pollFetchResut();

      if (fetchResult != null) {
        fetchTask = taskScheduler.getTasksMonitor().findPendingTask(fetchResult.getPriority(), fetchResult.getQueueId(), fetchResult.getItemId());

        if (fetchTask == null) {
          LOG.warn("Bad fetch item id {}-{}", fetchResult.getQueueId(), fetchResult.getItemId());
        }
      }
    }
    else {
      if (currPriority < 0 || currQueueId == null) {
        fetchTask = taskScheduler.schedule();
      }
      else {
        fetchTask = taskScheduler.schedule(currPriority, currQueueId);
      }

      if (fetchTask != null) {
        // the next time, we fetch items from the same queue as this time
        currPriority = fetchTask.getPriority();
        currQueueId = fetchTask.getQueueId();
        servedHosts.add(currQueueId);
      }
      else {
        // The current queue is empty, fetch item from top queue the next time
        currPriority = -1;
        currQueueId = null;
      }
    }

    return new FetchItem(fetchTask, fetchResult);
  }

  /**
   * Fetch one web page
   * */
  private FetchStatus fetchOne(FetchItem fetchItem) throws ProtocolNotFound, IOException {
    Protocol protocol = getProtocol(fetchItem);

    FetchTask task = fetchItem.task;
    FetchResult result = fetchItem.result;

    if (task == null) {
      return FetchStatus.Failed;
    }

    LOG.trace("Fetching <{}, {}>", task.getPriority(), task.getUrl());

    // Blocking until the target web page is loaded
    final ProtocolOutput output = protocol.getProtocolOutput(task.getUrl(), task.getPage());
    taskScheduler.finish(task.getPriority(), task.getQueueId(), task.getItemId(), output);

    if (debugContent) {
      cacheContent(result.getUrl(), result.getContent());
    }

    return FetchStatus.Success;
  }

  /**
   * Get network protocol, for example : http, ftp, sftp, and crowd protocol, etc
   * */
  private Protocol getProtocol(FetchItem fetchItem) throws ProtocolNotFound {
    FetchTask task = fetchItem.task;
    FetchResult result = fetchItem.result;

    Protocol protocol;
    if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
      protocol = protocolFactory.getCustomProtocol("crowd://" + task.getQueueId() + "/" + task.getItemId());

      protocol.setResult(result.getStatusCode(), result.getHeaders(), result.getContent());
    } else {
      if (task.getPage().getReprUrl() == null) {
        reprUrl = task.getUrl();
      } else {
        reprUrl = TableUtil.toString(task.getPage().getReprUrl());
      }

      // Block, open the network to fetch the web page and wait for a response
      protocol = this.protocolFactory.getProtocol(task.getUrl());
    }

    return protocol;
  }

  private void cacheContent(String url, byte[] content) {
    try {
      String date = new SimpleDateFormat("mm").format(new Date());
      Path path = Paths.get(Nutch.PATH_NUTCH_TMP_DIR, "cache", date, DigestUtils.md5Hex(url) + ".html");
      if (date.equals("00")) {
        // make a clean up every hour
        Files.delete(path.getParent());
      }

      Files.createDirectories(path.getParent());
      Files.write(path, content);
    }
    catch (IOException e) {
      LOG.error(e.toString());
    }
  }

  @Override
  public int compareTo(FetchThread fetchThread) {
    return id - fetchThread.id;
  }
} // FetcherThread
