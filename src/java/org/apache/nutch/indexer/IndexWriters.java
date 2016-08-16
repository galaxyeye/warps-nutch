package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.ObjectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/** Creates and caches {@link IndexWriter} implementing plugins. */
public class IndexWriters {

  public final static Logger LOG = LoggerFactory.getLogger(IndexWriters.class);

  private IndexWriter[] indexWriters;

  public IndexWriters(Configuration conf) {
    ObjectCache objectCache = ObjectCache.get(conf);

    synchronized (objectCache) {
      this.indexWriters = (IndexWriter[]) objectCache.getObject(IndexWriter.class.getName());
      if (this.indexWriters == null) {
        try {
          ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(IndexWriter.X_POINT_ID);
          if (point == null) {
            throw new RuntimeException(IndexWriter.X_POINT_ID + " not found.");
          }

          Extension[] extensions = point.getExtensions();
          HashMap<String, IndexWriter> indexerMap = new HashMap<>();
          for (int i = 0; i < extensions.length; i++) {
            Extension extension = extensions[i];
            IndexWriter writer = (IndexWriter) extension.getExtensionInstance();
            LOG.info("Adding " + writer.getClass().getName());
            if (!indexerMap.containsKey(writer.getClass().getName())) {
              indexerMap.put(writer.getClass().getName(), writer);
            }
          }

          objectCache.setObject(IndexWriter.class.getName(), indexerMap.values().toArray(new IndexWriter[0]));
        } catch (PluginRuntimeException e) {
          throw new RuntimeException(e);
        }

        this.indexWriters = (IndexWriter[]) objectCache.getObject(IndexWriter.class.getName());
      }
    }
  }

  public void open(Configuration conf) throws IOException {
    for (IndexWriter indexWriter : indexWriters) {
      indexWriter.open(conf);
    }
  }

  public void write(IndexDocument doc) throws IOException {
    for (IndexWriter indexWriter : indexWriters) {
      indexWriter.write(doc);
    }
  }

  public void update(IndexDocument doc) throws IOException {
    for (IndexWriter indexWriter : indexWriters) {
      indexWriter.update(doc);
    }
  }

  public void delete(String key) throws IOException {
    for (IndexWriter indexWriter : indexWriters) {
      indexWriter.delete(key);
    }
  }

  public void close() throws IOException {
    for (IndexWriter indexWriter : indexWriters) {
      indexWriter.close();
    }
  }

  public void commit() throws IOException {
    for (IndexWriter indexWriter : indexWriters) {
      indexWriter.commit();
    }
  }

  // lists the active IndexWriters and their configuration
  public String describe() throws IOException {
    StringBuffer buffer = new StringBuffer();
    if (indexWriters.length == 0) {
      buffer.append("No IndexWriters activated - check your configuration\n");
    }
    else {
      buffer.append("Active IndexWriters :\n");
    }

    for (IndexWriter indexWriter : indexWriters) {
      buffer.append(indexWriter.describe()).append("\n");
    }

    return buffer.toString();
  }
}
