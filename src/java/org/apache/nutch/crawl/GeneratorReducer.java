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
package org.apache.nutch.crawl;

import org.apache.avro.util.Utf8;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.fetcher.FetcherMapper;
import org.apache.nutch.mapreduce.NutchReducer;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reduce class for generate
 * 
 * The #reduce() method write a random integer to all generated URLs. This
 * random number is then used by {@link FetcherMapper}.
 * 
 */
public class GeneratorReducer extends NutchReducer<SelectorEntry, WebPage, String, WebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(GeneratorReducer.class);

  private enum Counter { rows, hostCountTooLarge, malformedUrl };

  protected static long count = 0;
  private long limit;
  private long maxCount;
  private boolean byDomain = false;
  private Map<String, Integer> hostCountMap = new HashMap<String, Integer>(); // TODO : better name?
  private Utf8 batchId;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    getCounter().register(Counter.class);

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY);

    limit = conf.getLong(Nutch.GENERATOR_TOP_N, Long.MAX_VALUE);
    limit /= context.getNumReduceTasks();

    maxCount = conf.getLong(Nutch.GENERATOR_MAX_COUNT, -2);
    batchId = new Utf8(conf.get(Nutch.GENERATOR_BATCH_ID, Nutch.ALL_BATCH_ID_STR));

    String countMode = conf.get(Nutch.GENERATOR_COUNT_MODE, Nutch.GENERATOR_COUNT_VALUE_HOST);
    if (countMode.equals(Nutch.GENERATOR_COUNT_VALUE_DOMAIN)) {
      byDomain = true;
    }

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "fetchMode", fetchMode,
        "batchId", batchId, 
        "limit", limit, 
        "maxCount", maxCount, 
        "byDomain", byDomain
    ));
  }

  @Override
  protected void reduce(SelectorEntry key, Iterable<WebPage> values, Context context) throws IOException, InterruptedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("generate reduce " + key.url);
    }

    for (WebPage page : values) {
      if (count >= limit) {
        stop("generated enought pages");
        return;
      }

      if (maxCount > 0) {
        countHosts(key.url);
      }

      Mark.INJECT_MARK.removeMarkIfExist(page);
      Mark.GENERATE_MARK.putMark(page, batchId);
      page.setBatchId(batchId);

      try {
        context.write(TableUtil.reverseUrl(key.url), page);

        getCounter().increase(Counter.rows);
        getCounter().updateAffectedRows(key.url);
      } catch (MalformedURLException e) {
        getCounter().increase(Counter.malformedUrl);
        continue;
      }

      ++count;
    } // for
  }

  private void countHosts(String url) throws MalformedURLException {
    String hostordomain = byDomain ? URLUtil.getDomainName(url) : URLUtil.getHost(url);

    Integer hostCount = hostCountMap.get(hostordomain);
    if (hostCount == null) {
      hostCountMap.put(hostordomain, 0);
      hostCount = 0;
    }

    if (hostCount >= maxCount) {
      getCounter().increase(Counter.hostCountTooLarge);
      return; // should stop?
    }

    hostCountMap.put(hostordomain, hostCount + 1);
  }
}
