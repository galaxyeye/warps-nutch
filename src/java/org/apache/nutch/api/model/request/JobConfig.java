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
package org.apache.nutch.api.model.request;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.nutch.api.JobManager.JobType;

import com.google.common.collect.Maps;

public class JobConfig {
  private String crawlId;
  private JobType type;
  private String confId;
  private String jobClassName;
  private Map<String, Object> args;

  public JobConfig() {
    
  }

  public JobConfig(String crawlId, JobType type, String confId) {
    this.crawlId = crawlId;
    this.type = type;
    this.confId = confId;
    args = Maps.newHashMap();
  }

  public String getCrawlId() {
    return crawlId;
  }

  public void setCrawlId(String crawlId) {
    this.crawlId = crawlId;
  }

  public JobType getType() {
    return type;
  }

  public void setType(JobType type) {
    this.type = type;
  }

  public String getConfId() {
    return confId;
  }

  public void setConfId(String confId) {
    this.confId = confId;
  }

  public Map<String, Object> getArgs() {
    return args;
  }

  public void setArgs(Map<String, Object> args) {
    this.args = args;
  }

  public String getJobClassName() {
    return jobClassName;
  }

  public void setJobClassName(String jobClass) {
    this.jobClassName = jobClass;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("{");
    sb.append("crawlId:" + crawlId + ", ");
    sb.append("type:" + type + ", ");
    sb.append("confId:" + confId + ", ");
    sb.append("jobClassName:" + jobClassName + ", ");
    sb.append("args:[");
    for (Entry<String, Object> arg : args.entrySet()) {
      sb.append(arg.getKey() + ":" + arg.getValue() + ", ");
    }
    sb.append("]");
    sb.append("}");

    return sb.toString();
  }
}
