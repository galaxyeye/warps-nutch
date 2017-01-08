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
package org.apache.nutch.jobs.samples;

import org.apache.hadoop.io.Text;
import org.apache.nutch.jobs.NutchReducer;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.common.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.nutch.jobs.NutchCounter.Counter.rows;

public class SampleReducer extends NutchReducer<Text, GoraWebPage, String, GoraWebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(SampleJob.class);

  public enum Counter { newRows }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);
    String batchId = conf.get(Nutch.PARAM_BATCH_ID);

    Params.of(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId
    ).withLogger(LOG).info();
  }

  @Override
  protected void reduce(Text key, Iterable<GoraWebPage> values, Context context) {
    try {
      doReduce(key, values, context);
    }
    catch(Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
  }

  private void doReduce(Text key, Iterable<GoraWebPage> values, Context context) throws IOException, InterruptedException {
    getCounter().increase(rows);

    String reversedUrl = key.toString();
    String url = TableUtil.unreverseUrl(reversedUrl);
    LOG.debug("Reduce : " + reversedUrl);

    for (GoraWebPage page : values) {
      for (int i = 0; i < 5; ++i) {
        page.getInlinks().put("http://www.example.com/reducer" + i, String.valueOf(i));
      }

      context.write(reversedUrl, page);
    }

    getCounter().updateAffectedRows(url);
  }
}
