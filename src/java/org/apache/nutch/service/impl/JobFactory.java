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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.jobs.*;
import org.apache.nutch.jobs.WebGraphUpdateJob;
import org.apache.nutch.service.JobManager.JobType;

import java.util.Map;

public class JobFactory {
  private static Map<JobType, Class<? extends NutchJob>> typeToClass;

  static {
    typeToClass = Maps.newHashMap();
    typeToClass.put(JobType.FETCH, FetchJob.class);
    typeToClass.put(JobType.GENERATE, GenerateJob.class);
    typeToClass.put(JobType.INDEX, IndexJob.class);
    typeToClass.put(JobType.INJECT, InjectJob.class);
    typeToClass.put(JobType.PARSE, ParserJob.class);
    typeToClass.put(JobType.UPDATEDB, WebGraphUpdateJob.class);
    typeToClass.put(JobType.READDB, WebTableReader.class);
    typeToClass.put(JobType.PARSECHECKER, ParserCheckJob.class);
  }

  public NutchJob createToolByType(JobType type, Configuration conf) {
    if (!typeToClass.containsKey(type)) {
      return null;
    }
    Class<? extends NutchJob> clz = typeToClass.get(type);
    return createTool(clz, conf);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public NutchJob createToolByClassName(String className, Configuration conf) {
    try {
      Class clz = Class.forName(className);
      return createTool(clz, conf);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  private NutchJob createTool(Class<? extends NutchJob> clz, Configuration conf) {
    return ReflectionUtils.newInstance(clz, conf);
  }

}
