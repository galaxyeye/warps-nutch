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

import org.apache.gora.store.DataStore;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.dbupdate.ReduceDatumBuilder;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;

public class DbUpdateReducer extends NutchReducer<UrlWithScore, NutchWritable, String, GoraWebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateReducer.class);

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public enum Counter { newRows };

  private ReduceDatumBuilder datumBuilder;
  private boolean additionsAllowed;
  private int maxLinks;
  public DataStore<String, GoraWebPage> datastore;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    this.datumBuilder = new ReduceDatumBuilder(getCounter(), conf);

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);
    additionsAllowed = conf.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);
    try {
      datastore = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
    }
    catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

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

  /**
   * We get a list of score datum who are inlinks to a webpage after partition,
   * so the dbupdate phrase calculates the scores of all pages
   * */
  private void doReduce(UrlWithScore key, Iterable<NutchWritable> values, Context context)
      throws IOException, InterruptedException {
    getCounter().increase(rows);

    String reversedUrl = key.getReversedUrl();
    String url = TableUtil.unreverseUrl(reversedUrl);

    // Calculate inlinked score data, and return the main web page
    WebPage page = datumBuilder.calculateInlinks(url, values);
    GoraWebPage oldPage = datastore.get(reversedUrl);

    datumBuilder.process(url, page, WebPage.wrap(oldPage), additionsAllowed);

    getCounter().updateAffectedRows(url);

    context.write(reversedUrl, page.get());
  }
}
