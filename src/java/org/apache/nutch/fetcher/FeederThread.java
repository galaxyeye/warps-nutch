package org.apache.nutch.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class feeds the fetchMonitor with input items, and re-fills them as
 * items are consumed by FetcherThread-s.
 */
class FeederThread extends Thread implements Comparable<FeederThread> {
  public static final Logger LOG = FetchJob.LOG;

  private static AtomicInteger instanceSequence = new AtomicInteger(0);

  private final int id;

  private final Configuration conf;
  @SuppressWarnings("rawtypes")
  private final Context context;

  private final FetchScheduler fetchScheduler;

  private final int feedLimit;
  private final long timeLimitMillis;

  private AtomicBoolean halted = new AtomicBoolean(false);
  private Iterator<FetchEntry> currentIter;
  private boolean hasMore;

  @SuppressWarnings("rawtypes")
  FeederThread(FetchScheduler fetchScheduler, Context context)
  throws IOException, InterruptedException {
    this.conf = context.getConfiguration();
    this.context = context;

    this.id = instanceSequence.incrementAndGet();

    this.fetchScheduler = fetchScheduler;

    this.feedLimit = fetchScheduler.getFeedLimit();

    this.setDaemon(true);
    this.setName(getClass().getSimpleName() + "-" + id);

    LOG.info("Initializing " + getName());

    hasMore = context.nextKey();
    if (hasMore) {
      currentIter = context.getValues().iterator();
    }

    // the value of the time limit is either -1 or the time where it should finish
    int timeLimitMins = conf.getInt("fetcher.timelimit.mins", -1);
    timeLimitMillis = timeLimitMins > 0 ? System.currentTimeMillis() + 1000 * 60 * timeLimitMins : -1;
  }

  public void halt() {
    halted.set(true);
  }

  public boolean isHalted() {
    return halted.get();
  }

  @Override
  public void run() {
    fetchScheduler.registerFeederThread(this);

    int feededCount = 0;
    int timeLimitCount = 0;

    try {
      FetchMonitor fetchMonitor = fetchScheduler.getFetchMonitor();

      while (!isHalted() && hasMore) {
        long now = System.currentTimeMillis();
        if (timeLimitMillis > 0 && now >= timeLimitMillis) {
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
        } // if

        int feedCapacity = feedLimit - fetchMonitor.readyItemCount() - fetchMonitor.pendingItemCount();
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
          fetchMonitor.produce(context.getJobID().getId(), url, entry.getWebPage());
          feedCapacity--;
          feededCount++;
        }

        if (currentIter.hasNext()) {
          continue; // finish items in current list before reading next key
        }

//        if (LOG.isDebugEnabled()) {
//          LOG.debug("Feeded " + feededCount + " input urls");
//        }

        hasMore = context.nextKey();
        if (hasMore) {
          currentIter = context.getValues().iterator();
        }
      } // while
    } catch (Throwable e) {
      LOG.error("QueueFeeder error reading input, record " + feededCount, e);
    }

    LOG.info("Feeder finished. Total " + feededCount + " records. Hit by time limit : " + timeLimitCount);

    context.getCounter(Nutch.COUNTER_GROUP_STATUS, "HitByTimeLimit-QueueFeeder").increment(timeLimitCount);

    fetchScheduler.unregisterFeederThread(this);
  }

  @Override
  public int compareTo(FeederThread feederThread) {
    return id - feederThread.id;
  }
}
