package org.apache.nutch.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.filter.CrawlFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCrawlFilter {

  private String[] detailUrls = {
    "http://mall.jumei.com/product_200918.html?from=store_lancome_list_items_7_4"
  };

  private CrawlFilters crawlFilters;

  /**
   * TODO : 
   * */
  @Before
  public void setUp() throws IOException {
    String crawlFilterRules = new String(Files.readAllBytes(Paths.get("/tmp/crawl_filters.json")));
    Configuration conf = NutchConfiguration.create();
    conf.set(CrawlFilters.CRAWL_FILTER_RULES, crawlFilterRules);

    crawlFilters = CrawlFilters.create(conf);
  }

  @Test
  public void testKeyRange() throws MalformedURLException {
    Map<String, String> keyRange = crawlFilters.getReversedKeyRanges();

    System.out.println(keyRange);

    assertTrue(keyRange.get("com.jumei.mall:http").equals("com.jumei.mall:http/\uFFFF"));
    assertTrue(keyRange.get("com.jumei.lancome:http/search.html").equals("com.jumei.lancome:http/search.html\uFFFF"));
    assertFalse(keyRange.get("com.jumei.lancome:http/search.html").equals("com.jumei.lancome:http/search.html\\uFFFF"));

    for (String detailUrl : detailUrls) {
      assertTrue(crawlFilters.testKeyRangeSatisfied(TableUtil.reverseUrl(detailUrl)));
    }
  }

  @Test
  public void testMaxKeyRange() throws MalformedURLException {
    String[] keyRange = crawlFilters.getMaxReversedKeyRange();
    System.out.println(keyRange[0] + ", " + keyRange[1]);

    assertTrue(Character.valueOf('\uFFFF') - Character.valueOf('a') == 65438);

    for (String detailUrl : detailUrls) {
      detailUrl = TableUtil.reverseUrl(detailUrl);

      assertTrue("com.jumei.lancome:http/search.html".compareTo(detailUrl) < 0);
      assertTrue("com.jumei.mall:http/\uFFFF".compareTo(detailUrl) > 0);
      // Note : \uFFFF, not \\uFFFF
      assertFalse("com.jumei.mall:http/\\uFFFF".compareTo(detailUrl) > 0);
    }

    for (String detailUrl : detailUrls) {
      detailUrl = TableUtil.reverseUrl(detailUrl);
      assertTrue(keyRange[0].compareTo(detailUrl) < 0);
      assertTrue(keyRange[1].compareTo(detailUrl) > 0);
    }
  }
}
