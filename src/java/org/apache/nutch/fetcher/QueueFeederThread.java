package org.apache.nutch.fetcher;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.fetcher.data.FetchItemQueues;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

/**
 * This class feeds the queues with input items, and re-fills them as
 * items are consumed by FetcherThread-s.
 */
public class QueueFeederThread extends Thread {
  public static final Logger LOG = FetcherJob.LOG;

  private final Configuration conf;
  @SuppressWarnings("rawtypes")
  private final Context context;
  private final FetchItemQueues queues;
  private final int feedLimit;
  private Iterator<FetchEntry> currentIter;
  boolean hasMore;
  private long timeLimitMillis = -1;

  @SuppressWarnings("rawtypes")
  public QueueFeederThread(Context context, FetchItemQueues queues, int feedLimit)
  throws IOException, InterruptedException {
    this.conf = context.getConfiguration();
    this.context = context;
    this.queues = queues;
    this.feedLimit = feedLimit;
    this.setDaemon(true);
    this.setName("QueueFeeder");

    LOG.debug("Initializing queue feeder...");

    hasMore = context.nextKey();
    if (hasMore) {
      currentIter = context.getValues().iterator();
    }

    // the value of the time limit is either -1 or the time where it should finish
    timeLimitMillis = System.currentTimeMillis() + 1000 * 60 * conf.getLong("fetcher.timelimit.mins", -1);
  }

  @Override
  public void run() {
    int feededCount = 0;
    int timeLimitCount = 0;

    try {
      while (hasMore) {
        long now = System.currentTimeMillis();
        if (timeLimitMillis > now && now >= timeLimitMillis) {
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

        int feedCapacity = feedLimit - queues.getReadyItemCount() - queues.getPendingItemCount();
        if (feedCapacity <= 0) {
          // queues are full - spin-wait until they have some free space
          try {
            Thread.sleep(1000);
          } catch (final Exception e) {};
          continue;
        }

//        LOG.debug("try feeding at most " + feedCapacity + " input urls ...");
//        if (LOG.isDebugEnabled() && (feedCapacity % 20 == 0)) {
//          LOG.debug("feeding " + feedCapacity + " input urls ...");
//        }

        while (feedCapacity > 0 && currentIter.hasNext()) {
          FetchEntry entry = currentIter.next();
          final String url = TableUtil.unreverseUrl(entry.getKey());
          queues.produceFetchItem(context.getJobID().getId(), url, entry.getWebPage());
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
    } catch (Exception e) {
      LOG.error("QueueFeeder error reading input, record " + feededCount, e);
      return;
    }

    LOG.info("QueueFeeder finished: total " + feededCount + " records. Hit by time limit : " + timeLimitCount);

    context.getCounter(Nutch.COUNTER_GROUP_STATUS, "HitByTimeLimit-QueueFeeder").increment(timeLimitCount);
  }
}
