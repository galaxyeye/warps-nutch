package org.apache.nutch.fetcher;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.fetcher.data.FetchItem;
import org.apache.nutch.fetcher.indexer.IndexManager;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * FetchManagerPool is shared by all Nutch Fetch Jobs
 * */
public class FetchManagerPool {

  public static final Logger LOG = LoggerFactory.getLogger(FetchManagerPool.class);

  private static FetchManagerPool instance;

  private String name;
  private Map<Integer, FetchManager> fetchManagers = Maps.newTreeMap();
  private Queue<Integer> jobIDs = Lists.newLinkedList();

  private FetchManagerPool() {
    this.name = this.getClass().getSimpleName() + "." + TimingUtil.now("d.Hms");

    LOG.info("Initialize fetch manager pool " + name);
  }

  public static FetchManagerPool getInstance() {
    if(instance == null) {
      instance = new FetchManagerPool();
    }

    return instance;
  }

  public String name() {
    return this.name;
  }

  public synchronized FetchManager create(int jobID, IndexManager indexManager, NutchCounter counter, Reducer.Context context) throws IOException {
    FetchManager fetchManager = new FetchManager(jobID, indexManager, counter, context);
    put(fetchManager);
    return fetchManager;
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
