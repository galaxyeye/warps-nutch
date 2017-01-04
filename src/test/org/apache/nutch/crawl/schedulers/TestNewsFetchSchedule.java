/*
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
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.filter.PageCategory;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.util.ConfigUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.nutch.metadata.Nutch.TCP_IP_STANDARDIZED_TIME;

/**
 * Test cases for AdaptiveFetchSchedule.
 * 
 */
public class TestNewsFetchSchedule {
  public static final Logger LOG = AbstractFetchSchedule.LOG;

  private float inc_rate;
  private float dec_rate;
  private Configuration conf;

  final Instant startTime = TCP_IP_STANDARDIZED_TIME.plus(365, ChronoUnit.DAYS);
  final Duration timeDelta = Duration.ofHours(2); // 2 hours
  // trigger the update of the page
  final Duration update = Duration.ofHours(24 * 2);

  Instant curTime = startTime;
  Instant lastModified = startTime;

  boolean changed = true;
  int miss = 0;
  int totalMiss = 0;
  int maxMiss = 0;
  int fetchCnt = 0;
  int changeCnt = 0;

  int totalRounds = 1000;
  int changeBefore = 500;

  WebPage p = WebPage.newWebPage();
  FetchSchedule fs = new NewsFetchSchedule();

  @Before
  public void setUp() throws Exception {
    conf = ConfigUtils.create();
    inc_rate = conf.getFloat("read.fetch.schedule.adaptive.inc_rate", 0.2f);
    dec_rate = conf.getFloat("read.fetch.schedule.adaptive.dec_rate", 0.2f);

    lastModified = startTime;

    fs.setConf(ConfigUtils.create());

    p.setBaseUrl("http://www.example.com");
    p.setStatus(0);
    p.setScore(1.0f);
    p.setFetchTime(startTime.plusSeconds(60));
    p.setFetchInterval(Duration.ofDays(1));

    choosePageType("seed", p);

    LOG.info(p.toString());
  }

  @Test
  public void testFetchSchedule() throws Exception {
    // let's move the timeline a couple of deltas
    for (int i = 0; i < totalRounds; i++) {
      if (lastModified.plus(update).isBefore(curTime)) {
        if (i < changeBefore) {
          // TableUtil.updateRefPublishTime(p, curTime.minus(2, ChronoUnit.DAYS));
          p.updateRefPublishTime(curTime);
          p.increaseRefArticles(1);
          p.increaseRefChars(2500);
          updateScore(p);

          changed = true;
          changeCnt++;
        }

        lastModified = curTime;
      }

      LOG.info("");
      LOG.info(i + ". " + (changed ? "[changed] " : "[no-change] ")
          + "Last Modified : " + getRoundNumber(p, p.getModifiedTime(), timeDelta) + ", "
          + "Next fetch : " + getRoundNumber(p, p.getFetchTime(), timeDelta) + ", "
          + "Interval : " + getRoundString(p, timeDelta)
          + "Score : " + p.getScore() + ", "
          + "Missed " + miss);

      if (p.getFetchTime().isBefore(curTime)) {
        fetchCnt++;
        fs.setFetchSchedule("http://www.example.com", p,
            p.getFetchTime(),
            p.getModifiedTime(),
            curTime,
            lastModified,
            changed ? FetchSchedule.STATUS_MODIFIED : FetchSchedule.STATUS_NOTMODIFIED);

        LOG.info("\tFetched & Adjusted: " + ", "
            + "Last Modified : " + getRoundNumber(p, p.getModifiedTime(), timeDelta) + ", "
            + "Next fetch : " + getRoundNumber(p, p.getFetchTime(), timeDelta) + ", "
            + "Score : " + p.getScore() + ", "
            + "Interval : " + getRoundString(p, timeDelta));

        p.increaseFetchCount();

        if (!changed) miss++;
        if (miss > maxMiss) maxMiss = miss;

        changed = false;
        totalMiss += miss;
        miss = 0;
      }

      if (changed) {
        miss++;
      }

      // move time line
      curTime = curTime.plus(timeDelta);
    }

    LOG.info("Total missed: " + totalMiss + ", max miss: " + maxMiss);
    LOG.info("Page changed " + changeCnt + " times, fetched " + fetchCnt + " times.");
  }

  private void choosePageType(String type, WebPage p) {
    if ("seed".equalsIgnoreCase(type)) {
      p.markAsSeed();
    }
    else if ("detail".equalsIgnoreCase(type)) {
      p.setPageCategory(PageCategory.DETAIL);
      p.setPageCategoryLikelihood(0.9f);
    }
  }

  private long getRoundNumber(WebPage p, Instant time, Duration timeDelta) {
    return (time.toEpochMilli() - startTime.toEpochMilli()) / timeDelta.toMillis();
  }

  private String getRoundString(WebPage p, Duration timeDelta) {
    final DecimalFormat df = new DecimalFormat("0.00");

    long inteval = p.getFetchInterval().toMillis();

    float round = 1.0f * inteval / timeDelta.toMillis();
    float days = 1.0f * inteval / Duration.ofDays(1).toMillis();
    float hours = 1.0f * inteval / Duration.ofHours(1).toMillis();

    return df.format(round) + " rounds (" + (days >= 1 ? (df.format(days) + " days") : df.format(hours) + " hours") + "), ";
  }

  private void updateScore(WebPage p) {
    float f1 = 1.0f;
    float f2 = 2.0f;

    long ra = p.getRefArticles();
    long rc = p.getRefChars();

    p.setScore(p.getScore() + f1 * ra + f2 * ((rc - 1000) / 1000));
  }
}
