/**
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
 */

package org.apache.nutch.crawl.schedulers;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.time.Duration;

/**
 * @author Vincent Zhang
 */
public class SeedFirstFetchSchedule extends AdaptiveFetchSchedule {
  public static final Logger LOG = AbstractFetchSchedule.LOG;

  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  @Override
  public void setFetchSchedule(String url, WebPage page, long prevFetchTime,
                               long prevModifiedTime, long fetchTime, long modifiedTime, int state) {
    long refetchTime = fetchTime;
    long intervalSec = page.getFetchInterval();

    if (modifiedTime <= 0) {
      modifiedTime = fetchTime;
    }

    boolean changed = false;
    if (TableUtil.isSeed(page)) {
      intervalSec = adjustSeedFetchInterval(page, fetchTime, intervalSec);
      changed = true;
    }
    else if (TableUtil.veryLiklyDetailPage(page)) {
      // Detail pages are fetched only once
      TableUtil.setNoMoreFetch(page);
      intervalSec = (int) Duration.ofDays(365 * 100).getSeconds();
      changed = true;
    }
    else {
      // Discovery seed pages?
    }

    if (changed) {
      updateRefetchTime(page, intervalSec, refetchTime, prevModifiedTime, modifiedTime);
    }
    else {
      super.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, state);
    }
  }

  /**
   * Adjust fetch interval for article pages
   * */
  private long adjustSeedFetchInterval(WebPage page, long fetchTime, long intervalSec) {
    long referredPublishTime = TableUtil.getReferredPublishTime(page);

    long hours = Duration.ofMillis(fetchTime - referredPublishTime).toHours();
    if (hours <= 24) {
      // There are updates today, keep re-fetch the page in every crawl loop
      intervalSec = 1;
    }
    else if (hours <= 72) {
      // If there is not updates in 24 hours but there are updates in 72 hours, re-fetch the page a hour later
      intervalSec = SECONDS_OF_HOUR;
    }
    else {
      // If there is no any updates in 72 hours, check the page later
      intervalSec *= (1.0f + INC_RATE);
      if (intervalSec < SECONDS_OF_HOUR) {
        intervalSec = SECONDS_OF_HOUR;
      }
      // TODO : log see pages without updates for long time
    }

    if (intervalSec > SEED_MAX_INTERVAL) {
      intervalSec = SEED_MAX_INTERVAL;
    }

    return intervalSec;
  }
}
