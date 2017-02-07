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
import org.apache.nutch.persist.WebPage;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.nutch.metadata.Nutch.TCP_IP_STANDARDIZED_TIME;
import static org.apache.nutch.metadata.Nutch.YES_STRING;
import static org.apache.nutch.metadata.Mark.INACTIVE;

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

  public AdaptiveNewsFetchSchedule() { super(); }

  public AdaptiveNewsFetchSchedule(Configuration conf) {
    super();
    setConf(conf);
  }

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
      interval = adjustSeedFetchInterval(url, page, fetchTime, modifiedTime, state);
      // interval = MIN_INTERVAL;
      changed = true;
    }
    else if (page.veryLikeDetailPage(url)) {
      // Detail pages are fetched only once, once it's mark
      page.putMark(INACTIVE, YES_STRING);
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
  private Duration adjustSeedFetchInterval(String url, WebPage page, Instant fetchTime, Instant modifiedTime, int state) {
    int fetchCount = page.getFetchCount();
    if (fetchCount <= 1) {
      // Ref-parameters are not initialized yet
      return MIN_INTERVAL;
    }

    Duration interval = page.getFetchInterval();

    Instant refPublishTime = page.getRefPublishTime();
    long hours = ChronoUnit.HOURS.between(refPublishTime, fetchTime);
    if (hours > 360 * 24 * 10) {
      // Longer than 10 years, it's very likely the refPublishTime is wrong
      // return super.getFetchInterval(page, fetchTime, modifiedTime, state);
    }

    if (hours <= 24) {
      // There are updates today, keep re-fetch the page in every crawl loop
      interval = MIN_INTERVAL;
    }
    else if (hours <= 72) {
      // If there is not updates in 24 hours but there are updates in 72 hours, re-fetch the page a hour later
      interval = Duration.ofHours(1);
    }
    else {
      // If there is no any updates in 72 hours, check the page at least 1 hour later and increase fetch interval time by time
      // TODO : fetch it at night
      long inc = (long)(interval.getSeconds() * INC_RATE);

      interval = interval.plusSeconds(inc);
      if (interval.toHours() < 1) {
        interval = Duration.ofHours(1);
      }

      if (hours < 240) {
        // No longer than SEED_MAX_INTERVAL
        if (interval.compareTo(SEED_MAX_INTERVAL) > 0) {
          interval = SEED_MAX_INTERVAL;
        }
      }
      else {
        nutchMetrics.reportInactiveSeeds(hours + "H, " + url);
      }
    }

    return interval;
  }
}
