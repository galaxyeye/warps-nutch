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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FetchManagerPool is shared by all Nutch Fetch Jobs
 * */
public class FetchManagerPool {

  public static final Logger LOG = LoggerFactory.getLogger(FetchManagerPool.class);

  private AtomicInteger managerIdGenerater = new AtomicInteger(0);

  private static FetchManagerPool instance;

  private String name;
  private Map<Integer, FetchManager> fetchManagers = Maps.newTreeMap();
  private Queue<Integer> ids = Lists.newLinkedList();

  private FetchManagerPool() {
    this.name = this.getClass().getSimpleName() + "." + TimingUtil.now("d.Hms");

    LOG.info("Initialize fetch manager pool " + name);
  }

  public synchronized static FetchManagerPool getInstance() {
    if(instance == null) {
      instance = new FetchManagerPool();
    }

    return instance;
  }

  public synchronized String name() {
    return this.name;
  }

  public synchronized FetchManager create(IndexManager indexManager, NutchCounter counter, Reducer.Context context) throws IOException {
    FetchManager fetchManager = new FetchManager(generateManagerId(), indexManager, counter, context);
    put(fetchManager);
    return fetchManager;
  }

  public synchronized void put(FetchManager fetchManager) {
    int id = fetchManager.getId();

    fetchManagers.put(id, fetchManager);
    ids.add(id);

    LOG.info("Add fetch manager #" + id);
    LOG.info("status : " + __toString());
  }

  public synchronized FetchManager get(int id) {
    return fetchManagers.get(id);
  }

  public synchronized void remove(int id) {
    ids.remove(id);
    fetchManagers.remove(id);

    LOG.info("Remove fetch manager #" + id + " from pool");
    LOG.info("status : " + __toString());
  }

  public synchronized void clear() {
    ids.clear();
    fetchManagers.clear();
  }

  /**
   * Random get @param count fetch items from an iterative selected job
   * */
  public synchronized List<FetchItem.Key> randomFetchItems(int count) {
    List<FetchItem.Key> keys = Lists.newArrayList();

    Integer id = ids.poll();
    if (id == null) {
      // LOG.debug("No running fetcher job");
      return Lists.newArrayList();
    }

    try {
      FetchManager fetchManager = fetchManagers.get(id);
      if (fetchManager == null) {
        LOG.error("Failed to find a fetch manager using id #" + id);

        remove(id);
        return Lists.newArrayList();
      }

      for (FetchItem item : fetchManager.consumeFetchItems(count)) {
        keys.add(item.getKey());
      }
    }
    catch (Exception e) {
      LOG.error(e.toString());
    }

    ids.add(id); // put back to the queue

    return keys;
  }

  @Override
  public synchronized String toString() {
    return __toString();
  }

  private int generateManagerId() {
    return managerIdGenerater.incrementAndGet();
  }

  private String __toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("Job IDs : " + StringUtils.join(ids, ", "));
    sb.append("\tQueue Size : " + fetchManagers.size());

    return sb.toString();    
  }
}
