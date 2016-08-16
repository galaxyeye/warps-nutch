package org.apache.nutch.indexer;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Created by vincent on 16-8-1.
 */

public class IndexerOutputFormat extends OutputFormat<String, IndexDocument> {

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
    // return an empty outputcommitter
    return new OutputCommitter() {
      @Override
      public void setupTask(TaskAttemptContext arg0) throws IOException {
      }

      @Override
      public void setupJob(JobContext arg0) throws IOException {
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext arg0) throws IOException {
      }

      @Override
      public void abortTask(TaskAttemptContext arg0) throws IOException {
      }
    };
  }

  @Override
  public RecordWriter<String, IndexDocument> getRecordWriter(TaskAttemptContext contex)
      throws IOException, InterruptedException {
    // final IndexWriter[] writers =
    // NutchIndexWriterFactory.getNutchIndexWriters(job.getConfiguration());

    final IndexWriters writers = new IndexWriters(contex.getConfiguration());
    writers.open(contex.getConfiguration());

    return new RecordWriter<String, IndexDocument>() {
      @Override
      public void write(String key, IndexDocument doc) throws IOException {
        // TODO: Check Write Status for delete or write.
        writers.write(doc);
      }

      @Override
      public void close(TaskAttemptContext contex) throws IOException, InterruptedException {
        writers.close();
      }
    };
  }
}