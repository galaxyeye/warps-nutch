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

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.persistency.Persistent;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NutchMapper<K1, V1 extends Persistent, K2, V2> extends GoraMapper<K1, V1, K2, V2> {

  protected static final Logger LOG = LoggerFactory.getLogger(NutchMapper.class);

  protected Configuration conf;
  private boolean completed = false;
  private NutchCounter counter;
  private NutchReporter reporter;

  protected long startTime = System.currentTimeMillis();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    LOG.debug("--mapper setup--");

    conf = context.getConfiguration();
    counter = new NutchCounter(context);
    reporter = new NutchReporter(counter);

    LOG.info(StringUtil.formatParams(
        "startTime", TimingUtil.format(startTime),
        "hostname", counter.getHostname()
    ));
  }

  @Override
  public void run(Context context) {
    try {
      setup(context);

      doRun(context);
    } catch (Throwable e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    finally {
      cleanup(context);
    }
  }

  public void doRun(Context context) throws IOException, InterruptedException {
    LOG.debug("mapper running--");

    while (!completed && context.nextKeyValue()) {
      map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
  }

  @Override
  protected void cleanup(Context context) {
    LOG.debug("mapper cleanup--");

    reporter.stopReporter();

    LOG.info(StringUtil.formatParams(
        "finishTime", TimingUtil.format(System.currentTimeMillis()),
        "mapTime", TimingUtil.elapsedTime(startTime)
    ));
  }

  protected boolean completed() {
    return completed;
  }

  protected void abort() {
    completed = true;
  }

  protected void abort(String error) {
    LOG.error(error);
    completed = true;
  }

  protected void stop() {
    completed = true;
  }

  protected void stop(String info) {
    LOG.info(info);
    completed = true;
  }

  protected NutchCounter getCounter() {
    return counter;
  }
}
