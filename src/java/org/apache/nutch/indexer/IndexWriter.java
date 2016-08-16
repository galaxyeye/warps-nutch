package org.apache.nutch.indexer;

import java.io.IOException;

/**
 * Created by vincent on 16-8-1.
 */
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.plugin.Pluggable;

public interface IndexWriter extends Configurable, Pluggable {
  /** The name of the extension point. */
  final static String X_POINT_ID = IndexWriter.class.getName();

  void open(JobConf jobConf, String name) throws IOException;

  void open(Configuration conf) throws IOException;

  void write(IndexDocument doc) throws IOException;

  void delete(String key) throws IOException;

  void update(IndexDocument doc) throws IOException;

  void commit() throws IOException;

  void close() throws IOException;

  /**
   * Returns a String describing the IndexWriter instance and the specific
   * parameters it can take
   */
  String describe();
}
