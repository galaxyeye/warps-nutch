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
import java.util.Date;

import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.persistency.Persistent;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NutchReducer<K1, V1, K2, V2 extends Persistent> extends GoraReducer<K1, V1, K2, V2> {

  protected static final Logger LOG = LoggerFactory.getLogger(NutchReducer.class.getName());

  protected Configuration conf;
  private boolean completed = false;
  private NutchCounter counter;
  private NutchReporter reporter;

  protected long startTime = System.currentTimeMillis();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    LOG.debug("--reducer setup--");

    conf = context.getConfiguration();
    counter = new NutchCounter(context);
    reporter = new NutchReporter(counter);

    LOG.info(NutchUtil.printArgMap(
        "startTime", TimingUtil.format(startTime),
        "reducerTasks", context.getNumReduceTasks(),
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

  protected void doRun(Context context) throws IOException, InterruptedException {
    while (!completed && context.nextKey()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
    }
  }

  @Override
  protected void cleanup(Context context) {
    LOG.debug("--reducer cleanup--");

    reporter.stopReporter();

    LOG.info(NutchUtil.printArgMap(
        "finishTime", TimingUtil.format(System.currentTimeMillis()),
        "timeElapsed", TimingUtil.elapsedTime(startTime)
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

  protected NutchReporter getReporter() {
    return reporter;
  }
}
