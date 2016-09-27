package org.apache.nutch.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.crawl.NutchContext;

import java.io.IOException;

/**
 * Created by vincent on 16-9-24.
 */
public class ReduerContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements NutchContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context;

  public ReduerContextWrapper(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) {
    this.context = context;
  }

  @Override
  public Configuration getConfiguration() {
    return context.getConfiguration();
  }

  @Override
  public boolean nextKey() throws IOException, InterruptedException {
    return context.nextKey();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return context.nextKeyValue();
  }

  @Override
  public KEYIN getCurrentKey() throws IOException, InterruptedException {
    return context.getCurrentKey();
  }

  @Override
  public VALUEIN getCurrentValue() throws IOException, InterruptedException {
    return context.getCurrentValue();
  }

  @Override
  public void write(KEYOUT var1, VALUEOUT var2) throws IOException, InterruptedException {
    context.write(var1, var2);
  }

  @Override
  public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return context.getValues();
  }

  @Override
  public void setStatus(String var1) {
    context.setStatus(var1);
  }

  @Override
  public String getStatus() {
    return context.getStatus();
  }

  @Override
  public int getJobId() {
    return context.getJobID().getId();
  }
}
