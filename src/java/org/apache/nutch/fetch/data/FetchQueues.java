package org.apache.nutch.fetch.data;

import org.apache.nutch.fetch.FetchMonitor;
import org.slf4j.Logger;

import java.util.*;

/**
 * Created by vincent on 16-9-22.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class FetchQueues {

  public final Logger LOG = FetchMonitor.LOG;

  private final Map<String, FetchQueue> workingQueues = new HashMap<>();
  private final Map<String, FetchQueue> detachedQueues = new HashMap<>();
  private final PriorityQueue<FetchQueue> priorityWorkingQueues = new PriorityQueue<>();

  private int nextQueuePosition = 0;

  public boolean add(FetchQueue queue) {
    if (queue == null) {
      return false;
    }

    workingQueues.put(queue.getId(), queue);
    priorityWorkingQueues.add(queue);

    return true;
  }

  public FetchQueue get(String queueId) {
    return get(queueId, false);
  }

  public FetchQueue getMore(String queueId) {
    return get(queueId, true);
  }

  private FetchQueue get(String queueId, boolean includeDetached) {
    FetchQueue queue = null;

    if (queueId != null) {
      queue = workingQueues.get(queueId);

      if (queue == null && includeDetached) {
        queue = detachedQueues.get(queueId);
      }
    }

    return queue;
  }

  public FetchQueue peek() {
    return priorityWorkingQueues.peek();
  }

  /**
   * Null queue id means the queue with top priority
   * */
  public FetchQueue getOrPeek(String queueId) {
    FetchQueue queue = get(queueId);

    if (queue == null) {
      queue = allocateTopPriorityFetchQueue();
      if (LOG.isTraceEnabled()) {
        if (queue != null && workingQueues.size() > 5) {
          LOG.trace(String.format("Fetch queue allocated, readyCount : %s, pendingCount : %s, queueId : %s",
              queue.readyCount(), queue.pendingCount(), queue.getId()));
        }
      }
    }

    return queue;
  }

  public Iterator<FetchQueue> iterator() {
    return workingQueues.values().iterator();
  }

  public int size() {
    return workingQueues.size();
  }

  public boolean isEmpty() { return workingQueues.isEmpty(); }

  public void clear() {
    workingQueues.clear();
    priorityWorkingQueues.clear();
  }

  /**
   * Get a pending task, the task can be in working queues or in detached queues
   * */
  public FetchTask getPendingTask(String queueId, int itemId) {
    FetchQueue queue = getMore(queueId);

    if (queue == null) {
      LOG.warn("Failed to find queue in fetch queues for {}", queueId);
      return null;
    }

    return queue.getPendingTask(itemId);
  }

  public Set<String> keySet() {
    return workingQueues.keySet();
  }

  public Collection<FetchQueue> values() {
    return workingQueues.values();
  }

  /**
   * Detached queues do not serve any more
   * */
  public void detach(FetchQueue queue) {
    queue.detach();
    workingQueues.remove(queue.getId());
    detachedQueues.put(queue.getId(), queue);
  }

  public String getCostReport() {
    StringBuilder sb = new StringBuilder();
    workingQueues.values().stream()
        .sorted(Comparator.comparing(FetchQueue::averageTimeCost).reversed())
        .limit(50)
        .forEach(queue -> sb.append(queue.getCostReport()).append('\n'));
    return sb.toString();
  }

  public void dump(int limit) {
    LOG.info("Total " + workingQueues.size() + " working fetch queues");

    int i = 0;
    for (String id : workingQueues.keySet()) {
      if (i++ > limit) {
        break;
      }
      FetchQueue queue = workingQueues.get(id);

      if (queue.readyCount() > 0 || queue.pendingCount() > 0) {
        LOG.info(i + "...........Working Fetch Queue..............");
        queue.dump();
      }
    }

    LOG.info("Total " + detachedQueues.size() + " detached fetch queues");
    i = 0;
    for (String id : detachedQueues.keySet()) {
      if (i++ > limit) {
        break;
      }
      FetchQueue queue = detachedQueues.get(id);
      if (queue.pendingCount() > 0) {
        LOG.info(i + "...........Detached Fetch Queue..............");
        queue.dump();
      }
    }
  }

  private FetchQueue allocateTopPriorityFetchQueue() {
    if (priorityWorkingQueues.isEmpty()) {
      workingQueues.values().stream().filter(FetchQueue::hasTasks).forEach(priorityWorkingQueues::add);
    }

    FetchQueue queue = priorityWorkingQueues.poll();
    return queue == null ? null : workingQueues.get(queue.getId());
  }

  private FetchQueue allocateNextFetchItemQueue() {
    return allocateNextFetchItemQueue(0);
  }

  /**
   * TODO : priority is not implemented
   * */
  private FetchQueue allocateNextFetchItemQueue(int priority) {
    FetchQueue queue = null;

    Iterator<FetchQueue> it = workingQueues.values().iterator();

    // Skip n items
    int i = 0;
    while (i++ < nextQueuePosition && it.hasNext()) {
      queue = it.next();
    }

    // Find the first non-empty queue
    while (queue != null && queue.readyCount() <= 0 && it.hasNext()) {
      queue = it.next();
    }

    if (queue != null && queue.readyCount() <= 0) {
      queue = null;
    }

    ++nextQueuePosition;
    if (nextQueuePosition >= workingQueues.size()) {
      nextQueuePosition = 0;
    }

    return queue;
  }
}
