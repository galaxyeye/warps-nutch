package org.apache.nutch.fetch;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchContext;
import org.apache.nutch.fetch.data.FetchEntry;
import org.apache.nutch.storage.WrappedWebPage;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class feeds the fetchMonitor with input items, and re-fills them as
 * items are consumed by FetcherThread-s.
 */
public class FeederThread extends Thread implements Comparable<FeederThread> {
  private final Logger LOG = FetchMonitor.LOG;
  public static final Logger REPORT_LOG = NutchMetrics.REPORT_LOG;

  private static AtomicInteger instanceSequence = new AtomicInteger(0);

  private final int id;

  private final Configuration conf;

  private final NutchContext context;

  private final TaskScheduler taskScheduler;

  private final int feedLimit;
  private final Instant fetchJobTimeLimit;

  private AtomicBoolean halted = new AtomicBoolean(false);
  private Iterator<FetchEntry> currentIter;

  @SuppressWarnings("rawtypes")
  public FeederThread(TaskScheduler taskScheduler, NutchContext context)
  throws IOException, InterruptedException {
    this.conf = context.getConfiguration();
    this.context = context;

    this.id = instanceSequence.incrementAndGet();

    this.taskScheduler = taskScheduler;

    this.feedLimit = taskScheduler.getFeedLimit();

    this.setDaemon(true);
    this.setName(getClass().getSimpleName() + "-" + id);

    Duration fetchJobTimeout = NutchConfiguration.getDuration(conf, "fetcher.timelimit", Duration.ofHours(1));
    fetchJobTimeLimit = Instant.now().plus(fetchJobTimeout);

    LOG.info(Params.format(
        "className", getClass().getSimpleName(),
        "id", id
    ));
  }

  public void halt() {
    halted.set(true);
  }

  public void exitAndJoin() {
    halted.set(true);
    try {
      join();
    } catch (InterruptedException e) {
      LOG.error(e.toString());
    }
  }

  public boolean isHalted() {
    return halted.get();
  }

  @Override
  public void run() {
    taskScheduler.registerFeederThread(this);

    int feededCount = 0;
    int timeLimitCount = 0;

    try {
      boolean hasMore = context.nextKey();
      if (hasMore) {
        currentIter = context.getValues().iterator();
      }

      TasksMonitor tasksMonitor = taskScheduler.getTasksMonitor();

      while (!isHalted() && hasMore) {
        Instant now = Instant.now();
        if (now.isAfter(fetchJobTimeLimit)) {
          // enough .. lets' simply
          // read all the entries from the input without processing them
          while (currentIter.hasNext()) {
            currentIter.next();
            timeLimitCount++;
          }

          hasMore = context.nextKey();
          if (hasMore) {
            currentIter = context.getValues().iterator();
          }
          continue;
        } // if time exceeds

        int feedCapacity = feedLimit - tasksMonitor.readyTaskCount() - tasksMonitor.pendingTaskCount();
        if (feedCapacity <= 0) {
          // fetchMonitor are full - spin-wait until they have some free space
          try {
            Thread.sleep(1000);
          } catch (final Exception ignored) {}

          continue;
        }

        while (feedCapacity > 0 && currentIter.hasNext()) {
          FetchEntry entry = currentIter.next();
          final String url = TableUtil.unreverseUrl(entry.getKey());
          tasksMonitor.produce(context.getJobId(), url, WrappedWebPage.wrap(entry.getWebPage()));
          feedCapacity--;
          feededCount++;
        }

        if (currentIter.hasNext()) {
          continue; // finish items in current list before reading next key
        }

        hasMore = context.nextKey();
        if (hasMore) {
          currentIter = context.getValues().iterator();
        }
      } // while
    } catch (Throwable e) {
      LOG.error("QueueFeeder error reading input, record " + feededCount, e);
    }

    LOG.info("Feeder finished. Total " + feededCount + " records.");
    REPORT_LOG.info("Feeded total " + feededCount + " records.");

    if (timeLimitCount > 0) {
      LOG.info("Hit by time limit : " + timeLimitCount);
    }

    // context.getCounter(Nutch.STAT_RUNTIME_STATUS, "HitByTimeLimit-QueueFeeder").increment(timeLimitCount);

    taskScheduler.unregisterFeederThread(this);
  }

  @Override
  public int compareTo(FeederThread feederThread) {
    return feederThread == null ? -1 : id - feederThread.id;
  }
}
