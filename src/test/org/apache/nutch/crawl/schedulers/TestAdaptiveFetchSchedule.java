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
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.util.ConfigUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static junit.framework.TestCase.assertEquals;

/**
 * Test cases for AdaptiveFetchSchedule.
 * 
 */
public class TestAdaptiveFetchSchedule {

  private float inc_rate;
  private float dec_rate;
  private Configuration conf;
  private Instant curTime, lastModified;
  private Duration interval, calculateInterval;
  private int changed;

  @Before
  public void setUp() throws Exception {
    conf = ConfigUtils.create();
    inc_rate = conf.getFloat("db.fetch.schedule.adaptive.inc_rate", 0.2f);
    dec_rate = conf.getFloat("db.fetch.schedule.adaptive.dec_rate", 0.2f);
    interval = Duration.ofSeconds(100);
    lastModified = Instant.EPOCH;
  }

  /**
   * Test the core functionality of AdaptiveFetchSchedule.
   * 
   */
  @Test
  public void testAdaptiveFetchSchedule() {
    FetchSchedule fs = new AdaptiveFetchSchedule();
    fs.setConf(conf);

    WebPage p = prepareWebpage();

    changed = FetchSchedule.STATUS_UNKNOWN;
    fs.setFetchSchedule("http://www.example.com", p, p.getFetchTime(), p.getModifiedTime(), curTime, lastModified, changed);
    validateFetchInterval(changed, p.getFetchInterval());

    changed = FetchSchedule.STATUS_MODIFIED;
    fs.setFetchSchedule("http://www.example.com", p, p.getFetchTime(), p.getModifiedTime(), curTime, lastModified, changed);
    validateFetchInterval(changed, p.getFetchInterval());
    p.setFetchInterval((int)interval.getSeconds());

    changed = FetchSchedule.STATUS_NOTMODIFIED;
    fs.setFetchSchedule("http://www.example.com", p, p.getFetchTime(), p.getModifiedTime(), curTime, lastModified, changed);
    validateFetchInterval(changed, p.getFetchInterval());
  }

  /**
   * Prepare a Webpage to Test Adaptive Fetch Schedule.
   * 
   * @return wp :Webpage
   */
  public WebPage prepareWebpage() {
    WebPage wp = WebPage.newWebPage();
    wp.setStatus(1);
    wp.setScore(1.0f);

    wp.setFetchTime(Instant.EPOCH);
    wp.setFetchInterval((int)interval.getSeconds());

    return wp;
  }

  /**
   * 
   * The Method validates Interval values according to changed parameter.
   * 
   * @param changed
   *          status value to check calculated IntervalValue.
   * @param expectedInterval
   *          to test IntervalValue get from webpage. Which is calculated via
   *          AdaptiveFetch Algorithm.
   */
  private void validateFetchInterval(int changed, Duration expectedInterval) {
    if (changed == FetchSchedule.STATUS_UNKNOWN) {
      assertEquals(expectedInterval, interval);
    } else if (changed == FetchSchedule.STATUS_MODIFIED) {
      calculateInterval = interval.minusSeconds((long)(interval.getSeconds() * dec_rate));
      assertEquals(expectedInterval, calculateInterval);
    } else if (changed == FetchSchedule.STATUS_NOTMODIFIED) {
      calculateInterval = interval.plusSeconds((long)(interval.getSeconds() * dec_rate));
      assertEquals(expectedInterval, calculateInterval);
    }
  }
}
