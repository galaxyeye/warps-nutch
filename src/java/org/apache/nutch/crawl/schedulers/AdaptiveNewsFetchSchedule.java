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

import org.apache.nutch.persist.WebPage;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * This class implements an adaptive re-fetch algorithm.
 * <p>
 * NOTE: values of DEC_FACTOR and INC_FACTOR higher than 0.4f may destabilize
 * the algorithm, so that the fetch interval either increases or decreases
 * infinitely, with little relevance to the page changes. Please use
 * {@link TestNewsFetchSchedule#testFetchSchedule()} method to test the values before applying them in a
 * production system.
 * </p>
 *
 * @author Vincent Zhang
 */
public class AdaptiveNewsFetchSchedule extends AdaptiveFetchSchedule {
  public static final Logger LOG = AbstractFetchSchedule.LOG;

  @Override
  public void setFetchSchedule(String url, WebPage page,
                               Instant prevFetchTime, Instant prevModifiedTime,
                               Instant fetchTime, Instant modifiedTime, int state) {
    Duration interval = page.getFetchInterval();
    if (modifiedTime.isBefore(TCP_IP_STANDARDIZED_TIME)) {
      modifiedTime = fetchTime;
    }

    boolean changed = false;
    if (page.isSeed()) {
      interval = adjustSeedFetchInterval(url, page, fetchTime, interval);
      changed = true;
    }
    else if (page.veryLikeDetailPage(url)) {
      // Detail pages are fetched only once
      page.setNoMoreFetch();
      interval = Duration.ofDays(NEVER_FETCH_INTERVAL_DAYS);
      changed = true;
    }
    else {
      // Discovery seed pages?
    }

    if (changed) {
      updateRefetchTime(page, interval, fetchTime, prevModifiedTime, modifiedTime);
    }
    else {
      super.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, state);
    }
  }

  /**
   * Adjust fetch interval for article pages
   * */
  private Duration adjustSeedFetchInterval(String url, WebPage page, Instant fetchTime, Duration interval) {
    int fetchCount = page.getFetchCount();
    if (fetchCount <= 1) {
      // Ref-parameters are not initialized yet
      return MIN_INTERVAL;
    }

    Instant refPublishTime = page.getRefPublishTime();
    long hours = ChronoUnit.HOURS.between(refPublishTime, fetchTime);
    if (hours <= 24) {
      // There are updates today, keep re-fetch the page in every crawl loop
      interval = MIN_INTERVAL;
    }
    else if (hours <= 72) {
      // If there is not updates in 24 hours but there are updates in 72 hours, re-fetch the page a hour later
      interval = Duration.ofHours(1);
    }
    else {
      // If there is no any updates in 72 hours, check the page at least 1 hour later
      // TODO : fetch it at night
      long inc = (long)(interval.getSeconds() * INC_RATE);

      interval = interval.plusSeconds(inc);
      if (interval.toHours() < 1) {
        interval = Duration.ofHours(1);
      }
    }

    // No longer than SEED_MAX_INTERVAL
    if (interval.compareTo(SEED_MAX_INTERVAL) > 0) {
      // TODO : LOG non-updating seeds
      interval = SEED_MAX_INTERVAL;
    }

    return interval;
  }
}
