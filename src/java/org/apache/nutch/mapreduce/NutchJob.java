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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Job.JobState;
import org.apache.hadoop.mapreduce.counters.AbstractCounters;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public abstract class NutchJob extends Configured {

  private static final Logger LOG = LoggerFactory.getLogger(NutchJob.class);

  protected Map<String, Object> params = Maps.newConcurrentMap();
  protected Map<String, Object> status = Maps.newConcurrentMap();
  protected Map<String, Object> results = Maps.newConcurrentMap();

  protected long affectedRows = 0;
  protected Job currentJob;
  protected int numJobs = 1;
  protected int currentJobNum = 0;
  protected long startTime = System.currentTimeMillis();

  protected void setup(Map<String, Object> args) throws Exception {
    LOG.info("\n\n\n------------------------- " + getJobName() + " -------------------------");
    LOG.info("Job starting at " + TimingUtil.format(startTime));

    synchronized (status) {
      status.put("startTime", TimingUtil.format(startTime));
    }
  }

  protected void cleanup(Map<String, Object> args) {
    try {

      updateStatus();
      updateResults();

      LOG.info(NutchUtil.printArgMap(results));
      LOG.info("Affected rows : " + affectedRows);
    }
    catch (Throwable e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  protected abstract void doRun(Map<String, Object> args) throws Exception;

  public String getJobName() {
    if (currentJob == null) {
      String readableTime = new SimpleDateFormat("MMdd.hhmmss").format(startTime);
      return getClass().getSimpleName() + "-" + readableTime;
    }
    else {
      return currentJob.getJobName();
    }
  }

  /**
   * Runs the tool, using a map of arguments. May return results, or null
   */
  public Map<String, Object> run(Map<String, Object> args) {
    try {
      setup(args);

      currentJob = Job.getInstance(getConf(), getJobName());
      currentJob.setJarByClass(this.getClass());

      doRun(args);
    }
    catch(Throwable e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    finally {
      cleanup(args);
    }

    return results;
  }

  public long getAffectedRows() {
    return affectedRows;
  }

  /** Returns relative progress of the tool, a float in range [0,1]. */
  public float getProgress() {
    if (currentJob == null) {
      return 0.0f;
    }

    float res = 0;
    try {
      res = (currentJob.mapProgress() + currentJob.reduceProgress()) / 2.0f;
    } catch (IOException e) {
      LOG.warn(e.toString());
      res = 0;
    } catch (IllegalStateException e) {
      LOG.warn(e.toString());
      res = 0;
    }

    // take into account multiple jobs
    if (numJobs > 1) {
      res = (currentJobNum + res) / (float) numJobs;
    }

    return res;
  }

  /** Returns current status of the running tool. */
  public Map<String, Object> getParams() {
    return params;
  }

  /** Returns current status of the running tool. */
  public Map<String, Object> getResults() {
    return results;
  }

  /** Returns current status of the running tool. */
  public Map<String, Object> getStatus() {
    synchronized (status) {
      return status;
    }
  }

  public void updateStatus() {
    if (currentJob == null) {
      return;
    }

    try {
      if (currentJob.getStatus() == null || currentJob.isRetired()) {
        return;
      }

      synchronized (status) {
        status.putAll(NutchUtil.recordJobStatus(currentJob));
      }

      long totalPages = getCounterValue(Nutch.COUNTER_GROUP_STATUS, NutchCounter.Counter.totalPages.name());
      affectedRows = totalPages;
    } catch (Throwable e) {
      LOG.warn(e.toString());

      return;
    }
  }

  public void updateResults() throws IOException, InterruptedException {
    String finishTime = TimingUtil.format(System.currentTimeMillis());
    String timeElapsed = TimingUtil.elapsedTime(startTime);

    results.putAll(NutchUtil.toArgMap(
        "startTime", TimingUtil.format(startTime),
        "finishTime", finishTime,
        "timeElapsed", timeElapsed
    ));
  }

  public Job getCurrentJob() {
    return currentJob;
  }

  /**
   * Stop the job with the possibility to resume. Subclasses should override
   * this, since by default it calls {@link #killJob()}.
   * 
   * @return true if succeeded, false otherwise
   */
  public boolean stopJob() throws Exception {
    return killJob();
  }

  /**
   * Kill the job immediately. Clients should assume that any results that the
   * job produced so far are in inconsistent state or missing.
   * 
   * @return true if succeeded, false otherwise.
   * @throws Exception
   */
  public boolean killJob() throws Exception {
    if (currentJob != null && !currentJob.isComplete()) {
      try {
        currentJob.killJob();
        return true;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }
    return false;
  }

  protected void recordParams(Object... args) {
    this.params.putAll(NutchUtil.toArgMap(args));
  }

  protected void recordAndLogParams(Object... args) {
    recordParams(args);
    LOG.info(NutchUtil.printArgMap(params));
  }

  protected long getCounterValue(String group, String name) throws IOException, InterruptedException {
    if (currentJob == null || currentJob.getStatus().isRetired()) {
      LOG.warn("Current job is null or job is retired");
      return 0;
    }

    if (currentJob.getCounters() == null) {
      LOG.warn("No any counters");
      return 0;
    }

    Counter counter = currentJob.getCounters().findCounter(group, name);
    if (counter == null) {
      LOG.warn("Can not find counter, group : " + group + ", name : " + name);

      return 0;
    }

    return counter.getValue();
  }
}
