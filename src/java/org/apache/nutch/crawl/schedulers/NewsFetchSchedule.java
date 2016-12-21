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

import static org.apache.nutch.metadata.Nutch.NEVER_FETCH_INTERVAL_DAYS;
import static org.apache.nutch.metadata.Nutch.TCP_IP_STANDARDIZED_TIME;

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
public class NewsFetchSchedule extends AdaptiveFetchSchedule {
  public static final Logger LOG = AbstractFetchSchedule.LOG;

  public void setConf(Configuration conf) {
    super.setConf(conf);
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
      interval = MIN_INTERVAL;
      changed = true;
    }
    else if (page.veryLikeDetailPage()) {
      // Detail pages are fetched only once
      page.setNoFetch();
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
}
