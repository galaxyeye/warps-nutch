/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class NutchReporter extends Thread {

  public static final Logger LOG = LoggerFactory.getLogger(NutchReporter.class);

  @SuppressWarnings("rawtypes")
  protected final TaskInputOutputContext context;

  private NutchCounter counter;

  protected final Configuration conf;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private final AtomicBoolean silent = new AtomicBoolean(false);

  private int reportIntervalMillis;
  
  public NutchReporter(NutchCounter counter) {
    this.counter = counter;
    this.context = counter.getContext();
    this.conf = context.getConfiguration();
    this.reportIntervalMillis = 1000 * conf.getInt("nutch.counter.report.interval.sec", 10);

    String jobName = context.getJobName();
    jobName = StringUtils.substringBeforeLast(jobName, "-");
    jobName = jobName.replaceAll("(\\[.+\\])", "");

    final String name = "Reporter-" + jobName + "-" + counter.id();

    setName(name);
    setDaemon(true);

    startReporter();
  }

  /**
   * Set report interval in seconds
   * @param intervalSec report interval in second
   * */
  public void setReportInterval(int intervalSec) {
    this.reportIntervalMillis = 1000 * intervalSec;
  }

  public void silence() {
    this.silent.set(true);
  }

  public void startReporter() {
    if (!running.get()) {
      start();
      running.set(true);
    }
  }

  public void stopReporter() {
    running.set(false);

    silent.set(false);

    report();
  }

  @Override
  public void run() {
    LOG.info("Report thread started");

    do {
      try {
        sleep(reportIntervalMillis);
      } catch (InterruptedException e) {}

      report();
    }
    while (running.get());

    LOG.info("Report thread stopped");
  } // run

  private void report() {
    // Can only access variables in this thread
    counter.accumulateGlobalCounters();

    if (!silent.get()) {
      String status = counter.getStatusString();
      if (!status.isEmpty()) {
        LOG.info(status);
      }
    }
  }
} // ReportThread
