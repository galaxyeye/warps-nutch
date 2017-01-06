package org.apache.nutch.fetch.data;

import org.apache.nutch.fetch.FetchMonitor;
import org.slf4j.Logger;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by vincent on 16-9-22.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class FetchQueues extends AbstractQueue<FetchQueue> {

  public final Logger LOG = FetchMonitor.LOG;

  /** All fetch queues, indexed by priority, item with bigger priority comes first. */
  private final PriorityQueue<FetchQueue> priorityActiveQueues = new PriorityQueue<>(Comparator.reverseOrder());

  /** All fetch queues, indexed by queue id. */
  private final Map<FetchQueue.Key, FetchQueue> activeQueues = new HashMap<>();

  /** Retired queues do not serve any more, but the tasks can be find out by findExtend. */
  private final Map<FetchQueue.Key, FetchQueue> inactiveQueues = new HashMap<>();

  @Override
  public boolean add(FetchQueue fetchQueue) {
    if (fetchQueue == null) {
      return false;
    }

    priorityActiveQueues.add(fetchQueue);
    activeQueues.put(fetchQueue.getKey(), fetchQueue);

    if(priorityActiveQueues.size() != activeQueues.size()) {
      LOG.error("Inconsistent status : size of activeQueues and priorityActiveQueues do not match");
    }

    return true;
  }

  @Override
  public boolean offer(FetchQueue fetchQueue) {
    return add(fetchQueue);
  }

  @Override
  public FetchQueue poll() {
    FetchQueue queue = priorityActiveQueues.poll();
    if (queue != null) {
      activeQueues.remove(queue.getKey());
    }

    if(priorityActiveQueues.size() != activeQueues.size()) {
      LOG.error("Inconsistent status : size of activeQueues and priorityActiveQueues do not match");
    }

    return queue;
  }

  @Override
  public FetchQueue peek() {
    return priorityActiveQueues.peek();
  }

  @Override
  public boolean remove(Object fetchQueue) {
    if (fetchQueue == null || !(fetchQueue instanceof FetchQueue)) {
      return false;
    }

    FetchQueue queue = (FetchQueue)fetchQueue;
    priorityActiveQueues.remove(queue);
    activeQueues.remove(queue.getId());
    inactiveQueues.remove(queue.getId());

    return true;
  }

  @Override
  public Iterator<FetchQueue> iterator() { return priorityActiveQueues.iterator(); }

  @Override
  public int size() { return priorityActiveQueues.size(); }

  @Override
  public boolean isEmpty() { return priorityActiveQueues.isEmpty(); }

  @Override
  public void clear() {
    priorityActiveQueues.clear();
    activeQueues.clear();
    inactiveQueues.clear();
  }

  /**
   * Retired queues do not serve any more, but the tasks can be find out and finished.
   * The tasks in detached queues can be find out to finish.
   *
   * A queue should be detached if
   * 1. the queue is too slow, or
   * 2. all tasks are done
   * */
  public void disable(FetchQueue queue) {
    priorityActiveQueues.remove(queue);
    activeQueues.remove(queue.getKey());

    queue.disable();
    inactiveQueues.put(queue.getKey(), queue);
  }

  public boolean hasPriorPendingTasks(int priority) {
    boolean hasPrior = false;
    for (FetchQueue queue : priorityActiveQueues) {
      if (queue.getPriority() < priority) {
        break;
      }

      hasPrior = queue.hasPendingTasks();
    }

    final Predicate<FetchQueue> p = queue -> queue.getPriority() >= priority && queue.hasPendingTasks();
    return hasPrior || inactiveQueues.values().stream().anyMatch(p);
  }

  public FetchQueue find(FetchQueue.Key key) { return search(key, false); }

  public FetchQueue findExtend(FetchQueue.Key key) { return search(key, true); }

  public FetchQueue search(FetchQueue.Key key, boolean searchInactive) {
    FetchQueue queue = null;

    if (key != null) {
      queue = activeQueues.get(key);

      if (queue == null && searchInactive) {
        queue = inactiveQueues.get(key);
      }
    }

    return queue;
  }

  public String getCostReport() {
    StringBuilder sb = new StringBuilder();
    activeQueues.values().stream()
        .sorted(Comparator.comparing(FetchQueue::averageTimeCost).reversed())
        .limit(50)
        .forEach(queue -> sb.append(queue.getCostReport()).append('\n'));
    return sb.toString();
  }

  public void dump(int limit) {
    LOG.info("Start dump fetch queue status. "
        + "Active : " + activeQueues.size() + ", inactive : " + inactiveQueues.size()+ " ...");
    activeQueues.values().stream().limit(limit).filter(FetchQueue::hasTasks).forEach(FetchQueue::dump);
    inactiveQueues.values().stream().limit(limit).filter(FetchQueue::hasPendingTasks).forEach(FetchQueue::dump);
    LOG.info("Done.");
  }
}
