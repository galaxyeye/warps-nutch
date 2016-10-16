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

import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.dbupdate.ReduceDatumBuilder;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;

public class DbUpdateReducer extends NutchReducer<UrlWithScore, NutchWritable, String, WebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateReducer.class);

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public enum Counter { newRows };

  private ReduceDatumBuilder datumBuilder;
  private boolean additionsAllowed;
  private int maxLinks;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    this.datumBuilder = new ReduceDatumBuilder(getCounter(), conf);

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);
    additionsAllowed = conf.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);

    Params.of(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "additionsAllowed", additionsAllowed,
        "maxLinks", maxLinks
    ).merge(datumBuilder.getParams()).withLogger(LOG).info();
  }

  @Override
  protected void reduce(UrlWithScore key, Iterable<NutchWritable> values, Context context) {
    try {
      doReduce(key, values, context);
    }
    catch(Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
  }

  private void doReduce(UrlWithScore key, Iterable<NutchWritable> values, Context context)
      throws IOException, InterruptedException {
    getCounter().increase(rows);

    String reversedUrl = key.getReversedUrl().toString();
    String url = TableUtil.unreverseUrl(reversedUrl);

    WebPage page = null;
    List<ScoreDatum> inlinkedScoreData = datumBuilder.getInlinkedScoreData();
    inlinkedScoreData.clear();

    /**
     * Notice : @vincent Why the value type is not clear?
     * */
    for (NutchWritable nutchWritable : values) {
      Writable val = nutchWritable.get();
      if (val instanceof WebPageWritable) {
        page = ((WebPageWritable) val).getWebPage();
      } else {
        inlinkedScoreData.add((ScoreDatum) val);
        if (inlinkedScoreData.size() >= maxLinks) {
          LOG.info("Limit reached, skipping further inlinks for " + url);
          break;
        }
      }
    } // for

    if (page == null) {
      if (!additionsAllowed) {
        return;
      }

      page = datumBuilder.createNewRow(url);

      getCounter().increase(Counter.newRows);
    }

    datumBuilder.updateRow(url, page);

    getCounter().updateAffectedRows(url);

    context.write(reversedUrl, page);
  }
}
