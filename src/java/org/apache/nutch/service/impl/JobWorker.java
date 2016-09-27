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
package org.apache.nutch.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.util.NutchUtil;
import org.apache.nutch.service.ConfManager;
import org.apache.nutch.service.model.request.JobConfig;
import org.apache.nutch.service.model.response.JobInfo;
import org.apache.nutch.service.model.response.JobInfo.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class JobWorker implements Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);

  private NutchJob nutchJob;
  private JobInfo jobInfo;
  private JobConfig jobConfig;
  private AtomicBoolean running = new AtomicBoolean(false);

  public JobWorker(JobConfig jobConfig, Configuration conf, NutchJob nutchJob) {
    this.nutchJob = nutchJob;
    this.jobConfig = jobConfig;

    if (jobConfig.getConfId() == null) {
      jobConfig.setConfId(ConfManager.DEFAULT);
    }

    jobInfo = new JobInfo(NutchUtil.generateJobId(jobConfig, hashCode()), jobConfig, State.IDLE, "idle");
  }

  @Override
  public void run() {
    try {
      running.set(true);

      jobInfo.setState(State.RUNNING);
      jobInfo.setMsg("OK");
      jobInfo.setResult(nutchJob.run(jobInfo.getArgs()));
      jobInfo.setState(State.FINISHED);
    } catch (Exception e) {
      LOG.error("Cannot run job worker!", e);
      jobInfo.setMsg("ERROR: " + e.toString());
      jobInfo.setState(State.FAILED);
    }
    finally {
      running.set(false);
    }
  }

  public boolean stopJob() {
    jobInfo.setState(State.STOPPING);

    try {
      return nutchJob.stopJob();
    } catch (Exception e) {
      throw new RuntimeException("Cannot stop job with id " + jobInfo.getId(), e);
    }
  }

  public boolean killJob() {
    jobInfo.setState(State.KILLING);
    try {
      boolean result = nutchJob.killJob();
      jobInfo.setState(State.KILLED);
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Cannot kill job with id " + jobInfo.getId(), e);
    }
  }

  public NutchJob getJobWrapper() {
    return nutchJob;
  }

  public boolean isRunning() {
    return running.get();
  }

  public String getJobId() {
    return jobInfo.getId();
  }

  public JobInfo getJobInfo() {
    nutchJob.updateStatus();

    jobInfo.setJobName(nutchJob.getJobName());
    jobInfo.setAffectedRows(nutchJob.getAffectedRows());
    jobInfo.setProgress(nutchJob.getProgress());
    jobInfo.setParams(nutchJob.getParams());
    jobInfo.setStatus(nutchJob.getStatus());
    jobInfo.setResult(nutchJob.getResults());

    return jobInfo;
  }
}
