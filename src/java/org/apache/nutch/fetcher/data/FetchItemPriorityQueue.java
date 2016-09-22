package org.apache.nutch.fetcher.data;

import org.apache.nutch.fetcher.FetcherJob;
import org.slf4j.Logger;

import java.util.*;

/**
 * Created by vincent on 16-9-22.
 */
public class FetchItemPriorityQueue {

  public static final Logger LOG = FetcherJob.LOG;

  private final Map<String, FetchItemQueue> workingQueues = new HashMap<>();
  private final Map<String, FetchItemQueue> detachedQueues = new HashMap<>();
  private final PriorityQueue<FetchItemQueue> priorityWorkingQueues = new PriorityQueue<>();

  private int nextQueuePosition = 0;

  public boolean add(FetchItemQueue queue) {
    if (queue == null) {
      return false;
    }

    workingQueues.put(queue.getId(), queue);
    priorityWorkingQueues.add(queue);

    return true;
  }

  public FetchItemQueue get(String queueId) {
    return get(queueId, false);
  }

  public FetchItemQueue getMore(String queueId) {
    return get(queueId, true);
  }

  private FetchItemQueue get(String queueId, boolean includeDetached) {
    FetchItemQueue queue = null;

    if (queueId != null) {
      queue = workingQueues.get(queueId);

      if (queue == null && includeDetached) {
        queue = detachedQueues.get(queueId);
      }
    }

    return queue;
  }

  public FetchItemQueue peek() {
    return priorityWorkingQueues.peek();
  }

  public FetchItemQueue getOrPeek(String queueId) {
    FetchItemQueue queue = get(queueId);

    if (queue == null) {
      queue = allocateTopPriorityFetchItemQueue();
      if (queue != null && workingQueues.size() > 5) {
        LOG.info(String.format(
            "Fetch task queue is allocated. readyTasks : %s, fetchingTasks : %s, queueId : %s",
            queue.getFetchQueueSize(),
            queue.getFetchingQueueSize(),
            queue.getId()
        ));
      }
    }

    return queue;
  }

  public Iterator<FetchItemQueue> iterator() {
    return workingQueues.values().iterator();
  }

  public int size() {
    return workingQueues.size();
  }

  public void clear() {
    workingQueues.clear();
    priorityWorkingQueues.clear();
  }

  /**
   * Get a pending task, the task can be in working queues or in detached queues
   * */
  public FetchItem getPendingTask(String queueId, long itemId) {
    FetchItemQueue queue = get(queueId, true);

    if (queue == null) {
      return null;
    }

    return queue.getPendingTask(itemId);
  }

  public Set<String> keySet() {
    return workingQueues.keySet();
  }

  public Collection<FetchItemQueue> values() {
    return workingQueues.values();
  }

  /**
   * Slow queues do not serve any more
   * */
  public void detach(FetchItemQueue queue) {
    queue.detach();
    workingQueues.remove(queue.getId());
    detachedQueues.put(queue.getId(), queue);
  }

  public String getCostReport() {
    StringBuilder sb = new StringBuilder();
    workingQueues.values().stream().filter(FetchItemQueue::isSlow).forEach(queue -> {
      sb.append(queue.getCostReport());
      sb.append('\n');
    });
    return sb.toString();
  }

  public void dump(int limit) {
    LOG.info("Total " + workingQueues.size() + " working fetch queues");

    int i = 0;
    for (String id : workingQueues.keySet()) {
      if (i++ > limit) {
        break;
      }
      FetchItemQueue queue = workingQueues.get(id);

      if (queue.getFetchQueueSize() > 0 || queue.getFetchingQueueSize() > 0) {
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
      FetchItemQueue queue = detachedQueues.get(id);
      if (queue.getFetchingQueueSize() > 0) {
        LOG.info(i + "...........Detached Fetch Queue..............");
        queue.dump();
      }
    }
  }

  private FetchItemQueue allocateTopPriorityFetchItemQueue() {
    if (priorityWorkingQueues.isEmpty() && !workingQueues.isEmpty()) {
      priorityWorkingQueues.addAll(workingQueues.values());
    }

    FetchItemQueue queue = priorityWorkingQueues.poll();
    if (queue == null) {
      LOG.error("Failed to poll a fetch item queue unexpected");
      return null;
    }

    return workingQueues.get(queue.getId());
  }

  private FetchItemQueue allocateNextFetchItemQueue() {
    return allocateNextFetchItemQueue(0);
  }

  /**
   * TODO : priority is not implemented
   * */
  private FetchItemQueue allocateNextFetchItemQueue(int priority) {
    FetchItemQueue queue = null;

    Iterator<FetchItemQueue> it = workingQueues.values().iterator();

    // Skip n items
    int i = 0;
    while (i++ < nextQueuePosition && it.hasNext()) {
      queue = it.next();
    }

    // Find the first non-empty queue
    while (queue != null && queue.getFetchQueueSize() <= 0 && it.hasNext()) {
      queue = it.next();
    }

    if (queue != null && queue.getFetchQueueSize() <= 0) {
      queue = null;
    }

    ++nextQueuePosition;
    if (nextQueuePosition >= workingQueues.size()) {
      nextQueuePosition = 0;
    }

    return queue;
  }
}
