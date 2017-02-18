package org.apache.nutch.fetch.indexer;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetch.FetchMonitor;
import org.apache.nutch.fetch.data.FetchTask;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexWriters;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.ParseStatus;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nutch.metadata.Nutch.DOC_FIELD_TEXT_CONTENT;

/**
 * Created by vincent on 16-8-23.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class JITIndexer {

  public static final Logger LOG = FetchMonitor.LOG;

  private Configuration conf;

  private int batchSize = 2000;
  private int indexThreadCount;
  private int minTextLenght;
  private AtomicInteger indexedPages = new AtomicInteger(0);
  private AtomicInteger ignoredPages = new AtomicInteger(0);

  private final Set<IndexThread> activeIndexThreads = new ConcurrentSkipListSet<>();
  private final BlockingQueue<FetchTask> indexTasks = Queues.newLinkedBlockingQueue(batchSize);
  private final IndexWriters indexWriters;

  private final LinkedList<IndexThread> indexThreads = Lists.newLinkedList();

  public JITIndexer(Configuration conf) throws IOException {
    this.conf = conf;

    this.batchSize = conf.getInt("indexer.index.batch.size", this.batchSize);
    this.indexThreadCount = conf.getInt("indexer.index.thread.count", 1);
    this.minTextLenght = conf.getInt("indexer.minimal.text.length", 300);

    indexWriters = new IndexWriters(conf);
    indexWriters.open(conf);
  }

  void registerFetchThread(IndexThread indexThread) {
    activeIndexThreads.add(indexThread);
  }

  void unregisterFetchThread(IndexThread indexThread) {
    activeIndexThreads.remove(indexThread);
  }

  public int getIndexThreadCount() {
    return indexThreadCount;
  }

  public int getIndexedPages() {
    return indexedPages.get();
  }

  public int getIngoredPages() {
    return ignoredPages.get();
  }

  /**
   * Add fetch item to index indexTasks
   * Thread safe
   */
  public void produce(FetchTask fetchTask) {
    WebPage page = fetchTask.getPage();
    if (page == null) {
      LOG.warn("Invalid FetchTask to index, ignore it");
      return;
    }

    if (!shouldProduce(page)) {
      return;
    }

    indexTasks.add(fetchTask);
  }

  /**
   * Thread safe
   */
  public FetchTask consume() {
    return indexTasks.poll();
  }

  public void cleanup() {
    indexThreads.forEach(IndexThread::halt);

    LOG.info("JITIndexer cleanup ...");
    LOG.info("There are " + ignoredPages + " not indexed short pages out of total " + indexedPages + " pages");

    try {
      FetchTask fetchTask = consume();
      while (fetchTask != null) {
        index(fetchTask);
        fetchTask = consume();
      }

      synchronized (indexWriters) {
        indexWriters.commit();
      }
    } catch (Exception e) {
      LOG.error(e.toString());
    } finally {
      synchronized (indexWriters) {
        indexWriters.close();
      }
    }
  }

  /**
   * Thread safe
   */
  public void index(FetchTask fetchTask) {
    try {
      if (fetchTask == null) {
        LOG.error("Failed to index, null fetchTask");
        return;
      }

      String url = fetchTask.getUrl();
      String reverseUrl = TableUtil.reverseUrl(url);
      WebPage page = fetchTask.getPage();

      IndexDocument doc = new IndexDocument.Builder(conf).build(reverseUrl, page);
      if (doc != null) {
        synchronized (indexWriters) {
          indexWriters.write(doc);
          page.putIndexTimeHistory(Instant.now());
        }
        indexedPages.incrementAndGet();
      } // if
    } catch (Throwable e) {
      LOG.error("Failed to index a page " + StringUtil.stringifyException(e));
    }
  }

  private boolean shouldIndex(IndexDocument doc) {
    String textContent = doc.getFieldValueAsString(DOC_FIELD_TEXT_CONTENT);
    if (textContent == null || textContent.length() < minTextLenght) {
      LOG.warn("Invalid text content to index, url : " + doc.getUrl());
      return false;
    }

    return true;
  }

  private boolean shouldProduce(WebPage page) {
    ParseStatus status = page.getParseStatus();

    if (!ParseStatusUtils.isSuccess(status) || status.getMajorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
      return false;
    }

    if (page.getContentText().length() < minTextLenght) {
      ignoredPages.incrementAndGet();
      return false;
    }

    return true;
  }
}
