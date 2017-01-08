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
package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.jobs.generate.GenerateJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.CrawlTestUtil;
import org.apache.nutch.util.NutchUtil;
import org.apache.nutch.util.TableUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

/**
 * Basic generator test. 1. Insert entries in webtable 2. Generates entries to
 * fetch 3. Verifies that number of generated urls match 4. Verifies that
 * highest scoring urls are generated
 * 
 */
public class TestGenerator extends AbstractNutchTest {

  public static final Logger LOG = LoggerFactory.getLogger(TestGenerator.class);

  private static String[] FIELDS = new String[] {
      GoraWebPage.Field.MARKERS.getName(),
      GoraWebPage.Field.SCORE.getName()
  };

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Test that generator generates fetchlist ordered by score (desc).
   * 
   * @throws Exception
   */
  @Test
  @Ignore("Temporarily disable until NUTCH-1572 is addressed.")
  public void testGenerateHighest() throws Exception {

    final int NUM_RESULTS = 2;

    ArrayList<WebPage> list = new ArrayList<>();

    for (int i = 0; i <= 100; i++) {
      list.add(createWebPage("http://aaa/" + pad(i), 1, i));
    }

    for (WebPage page : list) {
      datastore.put(page.url(), page.get());
    }
    datastore.flush();

    generateFetchlist(NUM_RESULTS, conf, false);

    ArrayList<WebPage> l = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    // sort urls by score desc
    Collections.sort(l, new ScoreComparator());

    // verify we got right amount of records
    assertEquals(NUM_RESULTS, l.size());

    // verify we have the highest scoring urls
    assertEquals("http://aaa/100", (l.get(0).url()));
    assertEquals("http://aaa/099", (l.get(1).url()));
  }

  private String pad(int i) {
    String s = Integer.toString(i);
    while (s.length() < 3) {
      s = "0" + s;
    }
    return s;
  }

  /**
   * Comparator that sorts by score desc.
   */
  public class ScoreComparator implements Comparator<WebPage> {

    public int compare(WebPage tuple1, WebPage tuple2) {
      if (tuple2.get().getScore() - tuple1.get().getScore() < 0) {
        return -1;
      }
      if (tuple2.get().getScore() - tuple1.get().getScore() > 0) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * Test that generator obeys the property "generate.max.count" and
   * "generate.count.mode".
   * 
   * @throws Exception
   */
  @Test
  @Ignore("Temporarily diable until NUTCH-1572 is addressed.")
  public void testGenerateHostLimit() throws Exception {
    ArrayList<WebPage> list = new ArrayList<>();

    list.add(createWebPage("http://www.example.com/index1.html", 1, 1));
    list.add(createWebPage("http://www.example.com/index2.html", 1, 1));
    list.add(createWebPage("http://www.example.com/index3.html", 1, 1));

    for (WebPage page : list) {
      datastore.put(TableUtil.reverseUrl(page.url()), page.get());
    }
    datastore.flush();

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(Nutch.PARAM_GENERATOR_MAX_TASKS_PER_HOST, 1);
    myConfiguration.set(Nutch.PARAM_GENERATOR_COUNT_MODE, Nutch.GENERATE_COUNT_VALUE_HOST);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    ArrayList<WebPage> fetchList = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    // verify we got right amount of records
    assertEquals(1, fetchList.size());

    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(Nutch.PARAM_GENERATOR_MAX_TASKS_PER_HOST, 2);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); // 3 as 2 + 1 skipped (already generated)

    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(Nutch.PARAM_GENERATOR_MAX_TASKS_PER_HOST, 3);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); // 3 as now all have generate mark
  }

  /**
   * Test that generator obeys the property "generator.max.count" and
   * "generator.count.value=domain".
   * 
   * @throws Exception
   */
  @Test
  @Ignore("Temporarily diable until NUTCH-1572 is addressed.")
  public void testGenerateDomainLimit() throws Exception {
    ArrayList<WebPage> list = new ArrayList<>();

    list.add(createWebPage("http://one.example.com/index.html", 1, 1));
    list.add(createWebPage("http://one.example.com/index1.html", 1, 1));
    list.add(createWebPage("http://two.example.com/index.html", 1, 1));
    list.add(createWebPage("http://two.example.com/index1.html", 1, 1));
    list.add(createWebPage("http://three.example.com/index.html", 1, 1));
    list.add(createWebPage("http://three.example.com/index1.html", 1, 1));

    for (WebPage page : list) {
      datastore.put(TableUtil.reverseUrl(page.url()), page.get());
    }
    datastore.flush();

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(Nutch.PARAM_GENERATOR_MAX_TASKS_PER_HOST, 1);
    myConfiguration.set(Nutch.PARAM_GENERATOR_COUNT_MODE, Nutch.GENERATE_COUNT_VALUE_DOMAIN);

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    ArrayList<WebPage> fetchList = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    // verify we got right amount of records
    assertEquals(1, fetchList.size());

    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(Nutch.PARAM_GENERATOR_MAX_TASKS_PER_HOST, 2);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); // 2 + 1 skipped (already generated)

    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(Nutch.PARAM_GENERATOR_MAX_TASKS_PER_HOST, 3);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    // verify we got right amount of records
    assertEquals(6, fetchList.size()); // 3 + 3 skipped (already generated)
  }

  /**
   * Test generator obeys the filter setting.
   * 
   * @throws Exception
   * @throws IOException
   */
  @Test
  @Ignore("Temporarily diable until NUTCH-1572 is addressed.")
  public void testFilter() throws Exception {
    ArrayList<WebPage> list = new ArrayList<>();

    list.add(createWebPage("http://www.example.com/index.html", 1, 1));
    list.add(createWebPage("http://www.example.net/index.html", 1, 1));
    list.add(createWebPage("http://www.example.org/index.html", 1, 1));

    for (WebPage page : list) {
      datastore.put(page.url(), page.get());
    }
    datastore.flush();

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.set("urlfilter.suffix.file", "filter-all.txt");

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, true);

    ArrayList<WebPage> fetchList = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    assertEquals(0, fetchList.size());

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(datastore, Mark.GENERATE, FIELDS);

    // verify nothing got filtered
    assertEquals(list.size(), fetchList.size());
  }

  /**
   * Generate Fetchlist.
   * 
   * @param numResults
   *          number of results to generate
   * @param config
   *          Configuration to use
   * @return path to generated batch
   * @throws IOException
   */
  private void generateFetchlist(int numResults, Configuration config, boolean filter) throws Exception {
    // generate batch
    GenerateJob g = new GenerateJob();
    g.setConf(config);
    String batchId = g.generate(numResults, "test", NutchUtil.generateBatchId(), false, System.currentTimeMillis(), filter, false);
    if (batchId == null)
      throw new RuntimeException("Generator failed");
  }

  /**
   * Constructs new {@link WebPage} from submitted parameters.
   * 
   * @param url
   *          url to use
   * @param fetchInterval
   * @param score
   * @return Constructed object
   */
  private WebPage createWebPage(final String url, final int fetchInterval, final float score) {
    GoraWebPage page = GoraWebPage.newBuilder().build();
    page.setFetchInterval(fetchInterval);
    page.setScore(score);
    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);
    return new WebPage(url, page, false);
  }
}
