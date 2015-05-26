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
package org.apache.nutch.api.impl;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.ConfManager;
import org.apache.nutch.api.JobManager;
import org.apache.nutch.api.NutchServer;
import org.apache.nutch.api.model.request.JobConfig;
import org.apache.nutch.api.model.response.JobInfo;
import org.apache.nutch.api.model.response.JobInfo.State;
import org.apache.nutch.mapreduce.NutchJob;
import org.slf4j.Logger;

public class RAMJobManager implements JobManager {

  public static final Logger LOG = NutchServer.LOG;

  private JobFactory jobFactory;
  private JobWorkerPoolExecutor executor;
  private ConfManager configManager;

  public RAMJobManager(JobFactory jobFactory, JobWorkerPoolExecutor executor, ConfManager configManager) {
    this.jobFactory = jobFactory;
    this.executor = executor;
    this.configManager = configManager;
  }

  /**
   * Create a new Job if there is no other running jobs for the given NutchConfig
   * We force only one job can be running under one NutchConfig, 
   * and the NutchConfig is associated with an unique UI Crawl
   * */
  @Override
  public String create(JobConfig jobConfig) {
    if (jobConfig.getArgs() == null) {
      throw new IllegalArgumentException("Arguments cannot be null!");
    }

    // Do not create if there is already a running worker
    String configId = jobConfig.getConfId();
    JobWorker worker = executor.findRunningWorkerByConfig(configId);
    if (worker != null) {
      throw new IllegalStateException("Another running job using config : " + configId);
    }

    Configuration conf = cloneConfiguration(configId);

    NutchJob nutchJob = createNutchJob(jobConfig, conf);
    worker = new JobWorker(jobConfig, conf, nutchJob);

    executor.execute(worker);
    executor.purge();

    return worker.getJobInfo().getId();
  }

  @Override
  public Collection<JobInfo> list(String crawlId, State state) {
    if (state == null || state == State.ANY) {
      return executor.getAllJobs();
    }

    if (state == State.RUNNING || state == State.IDLE) {
      return executor.getRunningJobs();
    }

    return executor.getRetiredJobs();
  }

  @Override
  public JobInfo get(String crawlId, String jobId) {
    JobWorker jobWorker = executor.findWorker(jobId);

    if (jobWorker != null) {
      return jobWorker.getJobInfo();
    }

    return new JobInfo(jobId, null, State.NOT_FOUND, "JOB NOT FOUND");
  }

  @Override
  public JobWorker getJobWorker(String crawlId, String jobId) {
    return executor.findWorker(jobId);
  }

  private Configuration cloneConfiguration(String confId) {
    Configuration conf = configManager.get(confId);
    if (conf == null) {
      throw new IllegalArgumentException("Unknown confId " + confId);
    }

    return new Configuration(conf);
  }

  private NutchJob createNutchJob(JobConfig jobConfig, Configuration conf) {
    if (StringUtils.isNotBlank(jobConfig.getJobClassName())) {
      return jobFactory.createToolByClassName(jobConfig.getJobClassName(), conf);
    }

    return jobFactory.createToolByType(jobConfig.getType(), conf);
  }

  @Override
  public boolean abort(String crawlId, String jobId) {    
    JobWorker jobWorker = executor.findWorker(jobId);
    if (jobWorker != null) {
      LOG.info("Kill Nutch Job " + jobId);
      jobWorker.killJob();
      return true;
    }

    LOG.info("No such Job " + jobId);

    return false;
  }

  @Override
  public boolean stop(String crawlId, String jobId) {
    JobWorker jobWorker = executor.findWorker(jobId);
    if (jobWorker != null) {
      LOG.info("Stop Nutch Job " + jobId);
      jobWorker.stopJob();
      return true;
    }

    LOG.info("No such Job " + jobId);

    return false;
  }
}
