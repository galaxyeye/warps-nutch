package org.apache.nutch.fetcher;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.fetcher.data.FetchTask;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * FetchSchedulers is shared by all Nutch Fetch Jobs
 * */
public class FetchSchedulers {

  public static final Logger LOG = LoggerFactory.getLogger(FetchSchedulers.class);

  private static FetchSchedulers instance;

  private final String name;
  private Map<Integer, FetchScheduler> fetchManagers = Maps.newTreeMap();
  private Queue<Integer> fetchSchedulerIds = Lists.newLinkedList();

  private FetchSchedulers() {
    this.name = this.getClass().getSimpleName() + "-" + TimingUtil.now("d.Hms");

    LOG.info("Initialize " + name);
  }

  public synchronized static FetchSchedulers getInstance() {
    if(instance == null) {
      instance = new FetchSchedulers();
    }

    return instance;
  }

  public synchronized String name() {
    return this.name;
  }

  public synchronized FetchScheduler create(NutchCounter counter, Reducer.Context context) throws IOException {
    FetchScheduler fetchScheduler = new FetchScheduler(counter, context);
    put(fetchScheduler);
    return fetchScheduler;
  }

  public synchronized void put(FetchScheduler fetchScheduler) {
    int id = fetchScheduler.getId();

    fetchManagers.put(id, fetchScheduler);
    fetchSchedulerIds.add(id);

    LOG.info("Add fetch manager #" + id);
    LOG.info("status : " + __toString());
  }

  public synchronized FetchScheduler get(int id) {
    return fetchManagers.get(id);
  }

  public synchronized void remove(int id) {
    fetchSchedulerIds.remove(id);
    fetchManagers.remove(id);

    LOG.info("Remove fetch manager #" + id + " from pool");
    LOG.info("status : " + __toString());
  }

  public synchronized void clear() {
    fetchSchedulerIds.clear();
    fetchManagers.clear();
  }

  /**
   * Random get @param count fetch items from an iterative selected job
   * */
  public synchronized List<FetchTask.Key> randomFetchItems(int count) {
    List<FetchTask.Key> keys = Lists.newArrayList();

    Integer id = fetchSchedulerIds.poll();
    if (id == null) {
      // LOG.debug("No running fetcher job");
      return Lists.newArrayList();
    }

    try {
      FetchScheduler fetchScheduler = fetchManagers.get(id);
      if (fetchScheduler == null) {
        LOG.error("Failed to find a fetch manager using id #" + id);

        remove(id);
        return Lists.newArrayList();
      }

      for (FetchTask item : fetchScheduler.schedule(count)) {
        keys.add(item.getKey());
      }
    }
    catch (Exception e) {
      LOG.error(e.toString());
    }

    fetchSchedulerIds.add(id); // put back to the queue

    return keys;
  }

  @Override
  public synchronized String toString() {
    return __toString();
  }

  private String __toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("Job IDs : " + StringUtils.join(fetchSchedulerIds, ", "));
    sb.append("\tQueue Size : " + fetchManagers.size());

    return sb.toString();    
  }
}
