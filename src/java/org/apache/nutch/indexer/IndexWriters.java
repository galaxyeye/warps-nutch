package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.ObjectCache;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

/** Creates and caches {@link IndexWriter} implementing plugins. */
public class IndexWriters {

  Logger LOG = IndexWriter.LOG;

  private IndexWriter[] indexWriters;

  public IndexWriters(Configuration conf) {
    ObjectCache objectCache = ObjectCache.get(conf);
    String cacheId = IndexWriter.class.getName();

    synchronized (objectCache) {
      this.indexWriters = (IndexWriter[]) objectCache.getObject(cacheId);
      if (this.indexWriters != null) {
        return;
      }

      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(IndexWriter.X_POINT_ID);
        if (point == null) {
          throw new RuntimeException(IndexWriter.X_POINT_ID + " not found.");
        }

        HashMap<String, IndexWriter> indexerMap = new HashMap<>();
        Extension[] extensions = point.getExtensions();
        for (Extension extension : extensions) {
          IndexWriter writer = (IndexWriter) extension.getExtensionInstance();
          String writerName = writer.getClass().getName();

          indexerMap.put(writerName, writer);
        }

        objectCache.setObject(cacheId, indexerMap.values().toArray(new IndexWriter[0]));
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      } catch (Throwable e) {
        LOG.error("Failed to load index writers, " + e.toString());
      }

      this.indexWriters = (IndexWriter[]) objectCache.getObject(cacheId);

      LOG.info("Active indexing writers : " + toString());
    }
  }

  public void open(Configuration conf) {
    for (IndexWriter indexWriter : indexWriters) {
      try {
        indexWriter.open(conf);
      }
      catch (Throwable e) {
        LOG.error("Failed to open indexer, " + e.toString());
      }
    }
  }

  public void write(IndexDocument doc) throws IOException {
    for (IndexWriter indexWriter : indexWriters) {
      indexWriter.write(doc);
    }
  }

  public void update(IndexDocument doc) {
    for (IndexWriter indexWriter : indexWriters) {
      try {
        indexWriter.update(doc);
      }
      catch (Throwable e) {
        LOG.error("Failed to update doc via indexer, " + e.getMessage());
      }
    }
  }

  public void delete(String key) {
    for (IndexWriter indexWriter : indexWriters) {
      try {
        indexWriter.delete(key);
      }
      catch (Throwable e) {
        LOG.error("Failed to delete doc via indexer, " + e.getMessage());
      }
    }
  }

  public void close() {
    for (IndexWriter indexWriter : indexWriters) {
      try {
        indexWriter.close();
      }
      catch (Throwable e) {
        LOG.error("Failed to close indexer, " + e.getMessage());
      }
    }
  }

  public void commit() throws IOException {
    for (IndexWriter indexWriter : indexWriters) {
      indexWriter.commit();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Arrays.stream(this.indexWriters).forEach(indexWriter -> sb.append(indexWriter.getClass().getSimpleName()).append(", "));
    return sb.toString();
  }
}
