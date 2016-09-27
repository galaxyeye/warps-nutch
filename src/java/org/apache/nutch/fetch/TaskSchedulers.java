package org.apache.nutch.fetch;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.crawl.NutchContext;
import org.apache.nutch.fetch.data.FetchTask;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * TaskSchedulers is shared by all Nutch Fetch Jobs
 * */
public class TaskSchedulers {

  public static final Logger LOG = LoggerFactory.getLogger(TaskSchedulers.class);

  private static TaskSchedulers instance;

  private final String name;
  private Map<Integer, TaskScheduler> fetchSchedulers = Maps.newTreeMap();
  private Queue<Integer> fetchSchedulerIds = Lists.newLinkedList();

  private TaskSchedulers() {
    this.name = this.getClass().getSimpleName() + "-" + TimingUtil.now("d.Hms");

    LOG.info("Initialize " + name);
  }

  public synchronized static TaskSchedulers getInstance() {
    if(instance == null) {
      instance = new TaskSchedulers();
    }

    return instance;
  }

  public synchronized String name() {
    return this.name;
  }

  public synchronized TaskScheduler create(NutchCounter counter, NutchContext context) throws IOException {
    TaskScheduler taskScheduler = new TaskScheduler(counter, context);
    put(taskScheduler);
    return taskScheduler;
  }

  public synchronized void put(TaskScheduler taskScheduler) {
    int id = taskScheduler.getId();

    fetchSchedulers.put(id, taskScheduler);
    fetchSchedulerIds.add(id);

    LOG.info("Add fetch manager #" + id);
    LOG.info("status : " + __toString());
  }

  public synchronized TaskScheduler get(int id) {
    return fetchSchedulers.get(id);
  }

  public synchronized void remove(int id) {
    fetchSchedulerIds.remove(id);
    fetchSchedulers.remove(id);

    LOG.info("Remove fetch manager #" + id + " from pool");
    LOG.info("status : " + __toString());
  }

  public synchronized void clear() {
    fetchSchedulerIds.clear();
    fetchSchedulers.clear();
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
      TaskScheduler taskScheduler = fetchSchedulers.get(id);
      if (taskScheduler == null) {
        LOG.error("Failed to find a fetch manager using id #" + id);

        remove(id);
        return Lists.newArrayList();
      }

      for (FetchTask item : taskScheduler.schedule(count)) {
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
    sb.append("\tQueue Size : " + fetchSchedulers.size());

    return sb.toString();    
  }
}
