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
package org.apache.nutch.jobs.generate;

import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import org.apache.nutch.common.Params;
import org.apache.nutch.jobs.NutchCounter;
import org.apache.nutch.jobs.NutchReducer;
import org.apache.nutch.jobs.fetch.FetchMapper;
import org.apache.nutch.jobs.generate.GenerateJob.SelectorEntry;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * Reduce class for generate
 *
 * The #reduce() method write a random integer to all generated URLs. This
 * random number is then used by {@link FetchMapper}.
 */
public class GenerateReducer extends NutchReducer<SelectorEntry, GoraWebPage, String, GoraWebPage> {

  public static final Logger LOG = GenerateJob.LOG;

  private enum Counter { hosts, hostCountTooLarge, malformedUrl,
    rowsInjected, rowsIsSeed, rowsIsFromSeed,
    pagesDepth0, pagesDepth1, pagesDepth2, pagesDepth3, pagesDepthN
  }

  private long limit;
  private long maxCountPerHost;
  private URLUtil.HostGroupMode hostGroupMode;
  private Multiset<String> hostNames = LinkedHashMultiset.create();
  private String batchId;
  private NutchMetrics nutchMetrics;

  private long count = 0;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    getCounter().register(Counter.class);

    String crawlId = conf.get(PARAM_CRAWL_ID);
    batchId = conf.get(PARAM_BATCH_ID, ALL_BATCH_ID_STR);

    // Generate top N links only
    limit = conf.getLong(PARAM_GENERATE_TOP_N, 100000);
    limit /= context.getNumReduceTasks();

    maxCountPerHost = conf.getLong(PARAM_GENERATE_MAX_TASKS_PER_HOST, 10000);
    hostGroupMode = conf.getEnum(PARAM_FETCH_QUEUE_MODE, URLUtil.HostGroupMode.BY_HOST);
    nutchMetrics = NutchMetrics.getInstance(conf);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "limit", limit,
        "maxCountPerHost", maxCountPerHost,
        "hostGroupMode", hostGroupMode
    ));
  }

  @Override
  protected void reduce(SelectorEntry key, Iterable<GoraWebPage> rows, Context context) throws IOException, InterruptedException {
    getCounter().increase(NutchCounter.Counter.rows);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Generate reduce " + key.url);
    }

    String url = key.url;

    String host = URLUtil.getHost(url, hostGroupMode);
    if (host == null) {
      getCounter().increase(Counter.malformedUrl);
      return;
    }

    for (GoraWebPage row : rows) {
      WebPage page = WebPage.wrap(url, row, false);

      try {
        if (count >= limit) {
          stop("Enough pages generated, quit");
          break;
        }

        addGeneratedHosts(host);
        if (maxCountPerHost > 0 && hostNames.count(host) > maxCountPerHost) {
          getCounter().increase(Counter.hostCountTooLarge);
          break;
        }

        int depth = page.getDepth();
        // seed pages are fetched imperative, so we just ignore topN parameter for them
        if (depth > 0) {
          ++count;
        }

        page = updatePage(url, page);

        context.write(TableUtil.reverseUrl(url), page.get());

        updateStatus(url, page, context);
      }
      catch (Throwable e) {
        LOG.error(StringUtil.stringifyException(e));
      }
    } // for
  }

  private WebPage updatePage(String url, WebPage page) {
    page.setBatchId(batchId);
    // Generate time, we will use this mark to decide if we re-generate this page
    page.setGenerateTime(startTime);
    page.setFetchPriority(page.sniffFetchPriority());

    page.removeMark(Mark.INJECT);
    page.putMark(Mark.GENERATE, batchId);

    return page;
  }

  private void updateStatus(String url, WebPage page, Context context) throws IOException {
    Counter counter;
    int depth = page.getDepth();
    if (depth == 0) {
      counter = Counter.pagesDepth0;
    }
    else if (depth == 1) {
      counter = Counter.pagesDepth1;
    }
    else if (depth == 2) {
      counter = Counter.pagesDepth2;
    }
    else if (depth == 3) {
      counter = Counter.pagesDepth3;
    }
    else {
      counter = Counter.pagesDepthN;
    }
    getCounter().increase(counter);

    // double check (depth == 0 or has IS-SEED metadata) , can be removed later
    if (page.isSeed()) {
      getCounter().increase(Counter.rowsIsSeed);
    }

    getCounter().updateAffectedRows(url);

    context.setStatus(getCounter().getStatusString());
  }

  @Override
  protected void cleanup(Context context) {
    try {
      LOG.info("Generated total " + hostNames.elementSet().size() + " hosts/domains");
      nutchMetrics.reportGeneratedHosts(hostNames.elementSet(), context.getJobName());
    }
    catch (Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
    finally {
      super.cleanup(context);
    }
  }

  private void addGeneratedHosts(String host) throws MalformedURLException {
    if (host == null) {
      return;
    }

    hostNames.add(host);

    getCounter().setValue(Counter.hosts, hostNames.entrySet().size());
  }
}
