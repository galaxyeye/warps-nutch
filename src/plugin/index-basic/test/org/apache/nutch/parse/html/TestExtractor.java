package org.apache.nutch.parse.html;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by vincent on 16-9-14.
 */
public class TestExtractor {

  @Test
  public void testRegexExtractor() {
    RegexExtractor extractor = new RegexExtractor();
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");
    extractor.setConf(conf);

    assertEquals("test-nutch", conf.get("http.agent.name"));
    assertEquals("regex-extractor.txt", conf.get("extractor.regex.rule.file"));

    Map<String, String> results = extractor.extract("", "责任编辑：王红强\n记者：张小明\n来源：网易新闻");
    assertEquals("张小明", results.get("author"));
    assertEquals("王红强", results.get("director"));
    assertEquals("网易新闻", results.get("reference"));
  }

  @Ignore
  @Test
  public void testCssPathExtractor() {
    CssPathExtractor extractor = new CssPathExtractor();
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");
    extractor.setConf(conf);

    assertEquals("test-nutch", conf.get("http.agent.name"));
    assertEquals("csspath-extractor.txt", conf.get("extractor.csspath.rule.file"));

    Map<String, String> results = extractor.extract("", "<html><body>" +
        "<div class='directory'>责任编辑：王红强</div>" +
        "<div class='author'>记者：张小明</div>" +
        "<div class='reference'>来源：网易新闻</div>" +
        "</body></html>");
    assertEquals("张小明", results.get("author"));
    assertEquals("王红强", results.get("director"));
    assertEquals("网易新闻", results.get("reference"));
  }
}
