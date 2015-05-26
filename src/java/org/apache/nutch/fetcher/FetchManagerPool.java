package org.apache.nutch.fetcher;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.fetcher.data.FetchItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * FetchManagerPool is shared by all Nutch Fetch Jobs
 * */
public class FetchManagerPool {

  public static final Logger LOG = LoggerFactory.getLogger(FetchManagerPool.class);

  private static FetchManagerPool instance;

  private Map<Integer, FetchManager> fetchManagers = Maps.newTreeMap();
  private Queue<Integer> jobIDs = Lists.newLinkedList();

  public static FetchManagerPool getInstance() {
    if(instance == null) {
      instance = new FetchManagerPool();
    }

    return instance;
  }

  public synchronized void put(FetchManager fetchManager) {
    int jobID = fetchManager.jobID();

    fetchManagers.put(jobID, fetchManager);
    jobIDs.add(jobID);

    LOG.info("Add fetch manager #" + jobID);
    LOG.info("status : " + __toString());
  }

  public synchronized FetchManager get(int jobID) {
    return fetchManagers.get(jobID);
  }

  /**
   * Random get @param count fetch items from an iterative selected job
   * @throws InterruptedException
   * */
  public synchronized List<FetchItem.Key> randomFetchItems(int count) {
    List<FetchItem.Key> keys = Lists.newArrayList();

    Integer jobID = jobIDs.poll();
    if (jobID == null) {
      // LOG.debug("No running fetcher job");
      return Lists.newArrayList();
    }

    try {
      FetchManager fetchManager = fetchManagers.get(jobID);
      if (fetchManager == null) {
        LOG.error("Failed to find a fetch manager using jobID #" + jobID);

        remove(jobID);
        return Lists.newArrayList();
      }

      for (FetchItem item : fetchManager.consumeFetchItems(count)) {
        keys.add(item.getKey());
      }
    }
    catch (Exception e) {
      LOG.error(e.toString());
    }

    jobIDs.add(jobID); // put back to the queue

    return keys;
  }

  public synchronized void remove(int jobID) {
    jobIDs.remove(jobID);
    fetchManagers.remove(jobID);

    LOG.info("Remove fetch manager #" + jobID + " from pool");
  }

  public synchronized void clear() {
    jobIDs.clear();
    fetchManagers.clear();
  }

  @Override
  public synchronized String toString() {
    return __toString();
  }

  private String __toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("Job IDs : " + StringUtils.join(jobIDs, ", "));
    sb.append("\tQueue Size : " + fetchManagers.size());

    return sb.toString();    
  }
}
