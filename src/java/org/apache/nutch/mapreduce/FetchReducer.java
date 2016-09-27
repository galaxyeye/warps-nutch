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

import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.crawl.NutchContext;
import org.apache.nutch.fetch.FetchMonitor;
import org.apache.nutch.fetch.TaskScheduler;
import org.apache.nutch.fetch.data.FetchEntry;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;

import java.io.IOException;

class FetchReducer extends NutchReducer<IntWritable, FetchEntry, String, WebPage> {

  public static final Logger LOG = FetchJob.LOG;

  private String jobName;
  private FetchMonitor fetchMonitor;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(TaskScheduler.Counter.class);

    jobName = context.getJobName();

    NutchContext nutchContext = new ReduerContextWrapper<IntWritable, FetchEntry, String, WebPage>(context);

    fetchMonitor = new FetchMonitor(jobName, getCounter(), nutchContext);
  }

  @Override
  protected void doRun(Context context) throws IOException, InterruptedException {
    fetchMonitor.run();
  }

  @Override
  protected void cleanup(Context context) {
    try {
      fetchMonitor.cleanup();
    }
    catch (Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
    finally {
      super.cleanup(context);
    }
  }
}
