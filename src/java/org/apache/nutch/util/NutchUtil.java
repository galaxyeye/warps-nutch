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
package org.apache.nutch.util;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.service.model.request.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Random;

public class NutchUtil {

  public static final Logger LOG = LoggerFactory.getLogger(NutchUtil.class);

  public static String generateBatchId() {
    return (System.currentTimeMillis() / 1000) + "-" + Math.abs(new Random().nextInt());
  }

  public static String generateConfigId() {
    return (System.currentTimeMillis() / 1000) + "-" + Math.abs(new Random().nextInt());
  }

  public static String generateJobId(JobConfig jobConfig, int hashCode) {
    if (jobConfig.getCrawlId() == null) {
      return MessageFormat.format("{0}-{1}-{2}", jobConfig.getConfId(),
          jobConfig.getType(), String.valueOf(hashCode));
    }

    return MessageFormat.format("{0}-{1}-{2}-{3}", jobConfig.getCrawlId(),
        jobConfig.getConfId(), jobConfig.getType(), String.valueOf(hashCode));
  }

  public static Map<String, Object> recordJobStatus(Job job) {
    Map<String, Object> jobStates = Maps.newHashMap();
    if (job == null) {
      return jobStates;
    }

    jobStates.putAll(getJobState(job, ArrayUtils.EMPTY_STRING_ARRAY));

    return jobStates;
  }

  public static Map<String, Object> getJobState(Job job, String... groups) {
    Map<String, Object> jobState = Maps.newHashMap();
    if (job == null) {
      return jobState;
    }

    try {
      if (job.getStatus() == null || job.isRetired()) {
        return jobState;
      }
    } catch (IOException | InterruptedException e) {
      return jobState;
    }

    jobState.put("jobName", job.getJobName());
    jobState.put("jobID", job.getJobID());

    jobState.put(Nutch.STAT_COUNTERS, getJobCounters(job, groups));

    return jobState;
  }

  public static Map<String, Object> getJobCounters(Job job, String... groups) {
    Map<String, Object> counters = Maps.newHashMap();
    if (job == null) {
      return counters;
    }

    try {
      for (CounterGroup group : job.getCounters()) {
        String groupName = group.getDisplayName();

        if (ArrayUtils.isEmpty(groups) || ArrayUtils.contains(groups, groupName)) {
          Map<String, Object> groupedCounters = Maps.newHashMap();
  
          for (Counter counter : group) {
            groupedCounters.put(counter.getName(), counter.getValue());
          }
  
          counters.put(groupName, groupedCounters);
        }
      }
    } catch (Exception e) {
      counters.put("error", e.toString());
    }

    return counters;
  }

  public static String getDomain(String url, String defaultHost) {
    String host = null;

    try {
      // if (queueMode == "byDomain") {}
      host = URLUtil.getDomainName(url);
    } catch (MalformedURLException ignored) {}

    return host == null ? defaultHost : host;
  }

}
