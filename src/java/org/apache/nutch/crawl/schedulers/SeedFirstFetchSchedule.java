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
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.nutch.metadata.Nutch.NEVER_FETCH_INTERVAL_DAYS;

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
    Instant refetchTime = Instant.ofEpochMilli(fetchTime);
    Duration interval = TableUtil.getFetchInterval(page);
    if (modifiedTime <= 0) {
      modifiedTime = fetchTime;
    }

    boolean changed = false;
    if (TableUtil.isSeed(page)) {
      interval = adjustSeedFetchInterval(page, Instant.ofEpochMilli(fetchTime), interval);
      changed = true;
    }
    else if (TableUtil.veryLikeDetailPage(page)) {
      // Detail pages are fetched only once
      TableUtil.setNoFetch(page);
      interval = Duration.ofDays(NEVER_FETCH_INTERVAL_DAYS);
      changed = true;
    }
    else {
      // Discovery seed pages?
    }

    if (changed) {
      updateRefetchTime(page, interval.getSeconds(), refetchTime.toEpochMilli(), prevModifiedTime, modifiedTime);
    }
    else {
      super.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, state);
    }
  }

  /**
   * Adjust fetch interval for article pages
   * */
  private Duration adjustSeedFetchInterval(WebPage page, Instant fetchTime, Duration interval) {
    int fetchCount = TableUtil.getFetchCount(page);
    if (fetchCount <= 1) {
      // Referred publish time is not initialized yet for this seed page
      return Duration.ofSeconds(1);
    }

    Instant referredPublishTime = TableUtil.getReferredPublishTime(page);
    if (referredPublishTime.isBefore(Instant.EPOCH)) {
      LOG.warn("Unexpected referred publish time : " + DateTimeUtil.format(referredPublishTime) + ", " + page.getBaseUrl());
    }

    // long hours = Duration.ofMillis(fetchTime - referredPublishTime).toHours();
    // long hours = Duration.between(referredPublishTime, fetchTime).toHours();
    long hours = ChronoUnit.HOURS.between(referredPublishTime, fetchTime);
    if (hours <= 24) {
      // There are updates today, keep re-fetch the page in every crawl loop
      interval = Duration.ofSeconds(1);
    }
    else if (hours <= 72) {
      // If there is not updates in 24 hours but there are updates in 72 hours, re-fetch the page a hour later
      interval = Duration.ofHours(1);
    }
    else {
      // If there is no any updates in 72 hours, check the page at least 1 hour later
      int adjust = (int)(interval.getSeconds() * INC_RATE);

      interval.plusSeconds(adjust);
      if (interval.toHours() < 1) {
        interval = Duration.ofHours(1);
      }
      // TODO : log see pages without updates for long time
    }

    // No longer than SEED_MAX_INTERVAL
    if (interval.compareTo(SEED_MAX_INTERVAL) > 0) {
      interval = SEED_MAX_INTERVAL;
    }

    return interval;
  }
}
