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
package org.apache.nutch.jobs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.nutch.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO : use metrics module, for example, http://metrics.dropwizard.io
 * */
public class NutchReporter extends Thread {

  private static final Logger LOG_ADDITIVITY = LoggerFactory.getLogger(NutchReporter.class + "Add");
  private static final Logger LOG_NON_ADDITIVITY = LoggerFactory.getLogger(NutchReporter.class);
  public static Logger LOG = LoggerFactory.getLogger(NutchReporter.class);

  public static Logger chooseLog(boolean additive) {
    // LOG = additive ? LOG_ADDITIVITY : LOG_NON_ADDITIVITY;
    return LOG;
  }

  @SuppressWarnings("rawtypes")
  protected final TaskInputOutputContext context;

  protected final Configuration conf;

  private final NutchCounter counter;

  private final String jobName;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private final AtomicBoolean silent = new AtomicBoolean(false);

  private int reportIntervalMillis;

  public NutchReporter(NutchCounter counter) {
    this.counter = counter;
    this.context = counter.getContext();
    this.conf = context.getConfiguration();
    this.reportIntervalMillis = 1000 * conf.getInt("nutch.counter.report.interval.sec", 10);

    this.jobName = context.getJobName();

    String simpleJobName = StringUtils.substringBeforeLast(jobName, "-");
    simpleJobName = simpleJobName.replaceAll("(\\[.+\\])", "");

    final String name = "Reporter-" + simpleJobName + "-" + counter.id();

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

    try {
      join();
    } catch (InterruptedException e) {
      LOG.error(e.toString());
    }
  }

  @Override
  public void run() {
    LOG.info("\n\n\n");
    String outerBorder = StringUtils.repeat('-', 100);
    String innerBorder = StringUtils.repeat('.', 100);
    LOG.info(outerBorder);
    LOG.info(innerBorder);
    LOG.info("== Report thread started [ " + DateTimeUtil.now() + " ] [ " + jobName + " ] ==");

    do {
      try {
        sleep(reportIntervalMillis);
      } catch (InterruptedException ignored) {}

      report();
    }
    while (running.get());

    LOG.info("Report thread stopped [ " + DateTimeUtil.now() + " ]");
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
