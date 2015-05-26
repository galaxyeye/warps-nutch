package org.apache.nutch.fetcher.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetcher.FetcherJob;
import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This class handles FetchItems which come from the same host ID (be it
 * a proto/hostname or proto/IP pair). It also keeps track of requests in
 * progress and elapsed time between requests.
 */
class FetchItemQueue {
  public static final Logger LOG = FetcherJob.LOG;

  List<FetchItem> fetchQueue = new LinkedList<FetchItem>();
  Map<Long, FetchItem>  fetchingQueue = new HashMap<Long, FetchItem>();

  AtomicLong nextFetchTime = new AtomicLong();

  long crawlDelay;
  long minCrawlDelay;
  int maxThreads;
  /**
   * Once timeout, the pending items should be put to the ready queue again.
   * time unit : seconds
   * */  
  long pendingTimeout;

  public FetchItemQueue(Configuration conf, int maxThreads, long crawlDelay, long minCrawlDelay, long pendingTimeout) {
    this.maxThreads = maxThreads;
    this.crawlDelay = crawlDelay;
    this.minCrawlDelay = minCrawlDelay;
    this.pendingTimeout = pendingTimeout;

    // ready to start
    setEndTime(System.currentTimeMillis() - crawlDelay);
  }

  public void produceFetchItem(FetchItem item) {
    if (item == null) return;
    fetchQueue.add(item);
  }

  public FetchItem consumeFetchItem() {
    if (fetchingQueue.size() >= maxThreads) return null;

    final long now = System.currentTimeMillis();
    if (nextFetchTime.get() > now) return null;

    if (fetchQueue.isEmpty()) return null;

    FetchItem item = null;
    try {
      item = fetchQueue.remove(0);
      addToFetchingQueue(item);
    } catch (final Exception e) {
      LOG.error("Cannot remove FetchItem from fetch queue or cannot add it to fetching queue", e);
    }

    return item;
  }

  /**
   * Reload pending fetch items so that the items can be re-fetched
   * 
   * In crowdsourcing mode, it's a common situation to lost
   * the fetching mission and should restart the task
   * 
   * @param force reload all pending fetch items immediately
   * */
  public void reviewPendingFetchItems(boolean force) {
    long now = System.currentTimeMillis();

    List<FetchItem> readyList = Lists.newArrayList();
    Map<Long, FetchItem> pendingList = Maps.newHashMap();

    final Iterator<FetchItem> it = fetchingQueue.values().iterator();
    while (it.hasNext()) {
      FetchItem item = it.next();

      if (force || now - item.getPendingStart() > this.pendingTimeout) {
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

  public void finishFetchItem(FetchItem item, boolean asap) {
    if (item != null) {
      fetchingQueue.remove(item.getItemID());
      setEndTime(System.currentTimeMillis(), asap);
    }
  }

  public void finishFetchItem(long itemID, boolean asap) {
    fetchingQueue.remove(itemID);
    setEndTime(System.currentTimeMillis(), asap);
  }

  public FetchItem getPendingFetchItem(long itemID) {
    return fetchingQueue.get(itemID);
  }

  public int getFetchQueueSize() {
    return fetchQueue.size();
  }

  public int getFetchingQueueSize() {
    return fetchingQueue.size();
  }

  public long getCrawlDelay() {
    return crawlDelay;
  }

  public boolean fetchingItemExist(long itemID) {
    return fetchingQueue.containsKey(itemID);
  }

  public int clearFetchQueue() {
    int presize = fetchQueue.size();
    fetchQueue.clear();
    return presize;
  }

  public void dump() {
    LOG.info("  maxThreads    = " + maxThreads);
    LOG.info("  fetchingQueue    = " + fetchingQueue.size());
    LOG.info("  crawlDelay    = " + crawlDelay);
    LOG.info("  minCrawlDelay = " + minCrawlDelay);
    LOG.info("  nextFetchTime = " + nextFetchTime.get());
    LOG.info("  now           = " + System.currentTimeMillis());
    for (int i = 0; i < fetchQueue.size(); i++) {
      final FetchItem it = fetchQueue.get(i);
      LOG.info("  " + i + ". " + it.getUrl());
    }
  }

  private void addToFetchingQueue(FetchItem item) {
    if (item == null) return;

    item.setPendingStart(System.currentTimeMillis());
    fetchingQueue.put(item.getItemID(), item);
  }

  private void setEndTime(long endTime) {
    setEndTime(endTime, false);
  }

  private void setEndTime(long endTime, boolean asap) {
    if (!asap)
      nextFetchTime.set(endTime + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
    else
      nextFetchTime.set(endTime);
  }
}
