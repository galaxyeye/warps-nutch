package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by vincent on 16-9-24.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public interface NutchContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  Configuration getConfiguration();
  boolean nextKey() throws IOException, InterruptedException;
  boolean nextKeyValue() throws IOException, InterruptedException;
  KEYIN getCurrentKey() throws IOException, InterruptedException;
  VALUEIN getCurrentValue() throws IOException, InterruptedException;
  void write(KEYOUT var1, VALUEOUT var2) throws IOException, InterruptedException;
  Iterable<VALUEIN> getValues() throws IOException, InterruptedException;
  void setStatus(String var1);
  String getStatus();
  int getJobId();
}
