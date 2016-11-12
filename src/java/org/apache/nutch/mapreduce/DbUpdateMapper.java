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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.dbupdate.MapDatumBuilder;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;

public class DbUpdateMapper extends NutchMapper<String, WebPage, UrlWithScore, NutchWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateMapper.class);

  public enum Counter { rowsMapped, newRowsMapped, notFetched, urlFiltered }

  private Configuration conf;

  private MapDatumBuilder datumFilter;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    conf = context.getConfiguration();

    getCounter().register(Counter.class);

    datumFilter = new MapDatumBuilder(getCounter(), conf);
  }

  /**
   * One row map to several rows
   * */
  @Override
  public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {
    getCounter().increase(rows);

    String url = TableUtil.unreverseUrl(reversedUrl);

    if (!Mark.FETCH_MARK.hasMark(page)) {
      getCounter().increase(Counter.notFetched);
      return;
    }

    url = datumFilter.filterUrl(page, url);
    if (url == null) {
      getCounter().increase(Counter.urlFiltered);
      return;
    }

    Pair<UrlWithScore, NutchWritable> mapDatum = datumFilter.buildDatum(reversedUrl, page);
    output(mapDatum.getKey(), mapDatum.getValue(), context);
    getCounter().increase(Counter.rowsMapped);

    Map<UrlWithScore, NutchWritable> updateData = datumFilter.createRowsFromOutlink(url, page);
    updateData.entrySet().forEach(e -> output(e.getKey(), e.getValue(), context));
    getCounter().increase(Counter.newRowsMapped, updateData.size());
  }

  private void output(UrlWithScore urlWithScore, NutchWritable nutchWritable, Context context) {
    try {
      context.write(urlWithScore, nutchWritable);
    } catch (IOException|InterruptedException e) {
      LOG.error("Failed to write to hdfs " + e.toString());
    }
  }
}
