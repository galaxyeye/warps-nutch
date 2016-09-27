package org.apache.nutch.fetch.indexer;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.mapreduce.FetchJob;
import org.apache.nutch.fetch.data.FetchTask;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexWriters;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by vincent on 16-8-23.
 */
public class JITIndexer {

  public static final Logger LOG = FetchJob.LOG;

  private Configuration conf;

  private int batchSize = 2500;
  private int indexThreadCount;

  private final Set<IndexThread> activeIndexThreads = new ConcurrentSkipListSet<>();
  private final BlockingQueue<FetchTask> indexTasks = Queues.newLinkedBlockingQueue(batchSize);
  private final IndexWriters indexWriters;

  private final LinkedList<IndexThread> indexThreads = Lists.newLinkedList();

  public JITIndexer(Configuration conf) throws IOException {
    this.conf = conf;

    this.batchSize = conf.getInt("indexer.index.batch.size", this.batchSize);
    this.indexThreadCount = conf.getInt("indexer.index.thread.count", 1);

    indexWriters = new IndexWriters(conf);
    indexWriters.open(conf);
  }

  void registerFetchThread(IndexThread indexThread) {
    activeIndexThreads.add(indexThread);
  }

  void unregisterFetchThread(IndexThread indexThread) {
    activeIndexThreads.remove(indexThread);
  }

  public int getIndexThreadCount() { return indexThreadCount; }

  /**
   * Add fetch item to index indexTasks
   * Thread safe
   * */
  public void produce(FetchTask item) {
    ParseStatus pstatus = item.getPage().getParseStatus();
    if (pstatus == null || !isParseSuccess(pstatus) || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
      // getCounter().increase(IndexMapper.Counter.unmatchStatus);
      return; // filter urls not parsed
    }

    indexTasks.add(item);
  }

  /**
   * Thread safe
   * */
  public FetchTask consume() {
    return indexTasks.poll();
  }

  public void cleanup() {
    indexThreads.stream().forEach(IndexThread::halt);

    LOG.info("JITIndexer cleanup...");

    try {
      FetchTask fetchTask = consume();
      while(fetchTask != null) {
        index(fetchTask);
        fetchTask = consume();
      }

      synchronized (indexWriters) {
        indexWriters.commit();
      }
    }
    catch (Exception e) {
      LOG.error(e.toString());
    }
    finally {
      indexWriters.close();
    }
  }

  /**
   * thread safety
   * */
  public void index(FetchTask fetchTask) {
    try {
      String url = fetchTask.getUrl();
      String key = TableUtil.reverseUrl(url);
      WebPage page = fetchTask.getPage();

      if (page != null) {
        IndexDocument doc = new IndexDocument.Builder(conf).build(key, page);
        if (doc != null) {
          synchronized (indexWriters) {
            indexWriters.write(doc);
          }
        } // if
      } // if
    }
    catch (IOException e) {
      LOG.error(e.toString());
    }
  }

  private static boolean isParseSuccess(ParseStatus status) {
    if (status == null) {
      return false;
    }
    return status.getMajorCode() == ParseStatusCodes.SUCCESS;
  }
}
