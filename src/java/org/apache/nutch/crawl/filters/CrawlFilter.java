/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.crawl.filters;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.net.RegexURLFilter;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.w3c.dom.Node;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

public class CrawlFilter extends Configured {

  public static final Logger LOG = CrawlFilters.LOG;

  public static final String CRAWL_FILTER_FILTER = "crawl.filter.filter";
  public static final String CRAWL_FILTER_NORMALISE = "crawl.filter.normalise";

  public enum PageType {
    INDEX, DETAIL, ANY;
  };

  @Expose
  private PageType pageType = PageType.ANY;
  @Expose
  private String urlRegexRule;
  @Expose
  private TextFilter textFilter;
  @Expose
  private BlockFilter blockFilter;
  @Expose
  private String startKey = "";
  @Expose
  private String endKey = "\uFFFF";

  private String reversedStartKey;
  private String reversedEndKey;

//  private boolean filter;
//  private boolean normalise;
//  private URLFilters filters;
//  private URLNormalizers normalizers;
  // additional url filter,
  // TODO : merge to filters
  private RegexURLFilter urlFilter;

  public CrawlFilter() {
    
  }

  public CrawlFilter(Configuration conf) {
    setConf(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf == null) {
      return;
    }

    super.setConf(conf);

    try {
//      filter = conf.getBoolean(CRAWL_FILTER_FILTER, true);
//      if (filter) {
//        filters = new URLFilters(conf);
//      }
//      normalise = conf.getBoolean(CRAWL_FILTER_NORMALISE, true);
//      if (normalise) {
//        normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
//      }

      // CrawlFilter specified RegexURLFilter
      if (urlRegexRule != null) {
        urlFilter = new RegexURLFilter(urlRegexRule);
      }

      // TODO : move to Table utils just like scent project does
      if (startKey != null) {
        startKey = startKey.replaceAll("\\u0001", "\u0001");
        startKey = startKey.replaceAll("\\\\u0001", "\u0001");

        reversedStartKey = TableUtil.reverseUrl(startKey);
      }

      if (endKey != null) {
        endKey = endKey.replaceAll("\\uFFFF", "\uFFFF");
        endKey = endKey.replaceAll("\\\\uFFFF", "\uFFFF");

        reversedEndKey = TableUtil.reverseUrl(endKey);
      }
    }
    catch(RuntimeException | IOException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  public boolean testUrlSatisfied(String url) {
    if (url == null) {
      return false;
    }

    boolean passed = true;

//    try {
//      if (normalizers != null) {
//        url = normalizers.normalize(url, URLNormalizers.SCOPE_DEFAULT);
//        passed = url != null;
//      }
//
//      if (passed && filters != null) {
//        url = filters.filter(url);
//        passed = url != null;
//      }
//
//      if (passed && urlFilter != null) {
//        url = urlFilter.filter(url);
//        passed = url != null;
//      }
//    } catch (MalformedURLException|URLFilterException e) {
//      LOG.error(e.toString());
//      passed = false;
//    }

    if (passed && urlFilter != null) {
      url = urlFilter.filter(url);
      passed = url != null;
    }

    return passed;
  }

  public boolean testKeyRangeSatisfied(String reversedUrl) {
    return keyGreaterEqual(reversedUrl, reversedStartKey) && keyLessEqual(reversedUrl, reversedEndKey);
  }

  public static boolean keyGreaterEqual(String test, String bound) {
    if (test == null) {
      return false;
    }

    if (bound == null) {
      return true;
    }

    return test.compareTo(bound) >= 0;
  }

  public static boolean keyLessEqual(String test, String bound) {
    if (test == null) {
      return false;
    }

    if (bound == null) {
      return true;
    }

    return test.compareTo(bound) <= 0;
  }

  public boolean testTextSatisfied(String text) {
    if (text == null) {
      return false;
    }

    if (textFilter == null) {
      return true;
    }

    return textFilter.test(text);
  }

  public boolean isAllowed(Node node) {
    if (blockFilter == null) {
      return true;
    }

    return blockFilter.isAllowed(node);
  }

  public boolean isDisallowed(Node node) {
    return !isAllowed(node);
  }

  public boolean isDetailUrl(String url) {
    return pageType.equals(PageType.DETAIL) && testUrlSatisfied(url);
  }

  public boolean isIndexUrl(String url) {
    return pageType.equals(PageType.INDEX) && testUrlSatisfied(url);
  }

  public PageType getPageType() {
    return pageType;
  }

  public void setPageType(PageType pageType) {
    this.pageType = pageType;
  }

  public String getUrlFilter() {
    return urlRegexRule;
  }

  public void setUrlFilter(String urlFilter) {
    this.urlRegexRule = urlFilter;
  }

  public TextFilter getTextFilter() {
    return textFilter;
  }

  public void setTextFilter(TextFilter textFilter) {
    this.textFilter = textFilter;
  }

  public BlockFilter getBlockFilter() {
    return blockFilter;
  }

  public void setBlockFilter(BlockFilter blockFilter) {
    this.blockFilter = blockFilter;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getReversedStartKey() {
    return reversedStartKey;
  }

  public void setStartKey(String startKey) {
    this.startKey = startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public String getReversedEndKey() {
    return reversedEndKey;
  }

  public void setEndKey(String endKey) {
    this.endKey = endKey;
  }

  @Override
  public String toString() {
    Gson gson = new GsonBuilder()
//        .disableHtmlEscaping()
        .excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);

//    StringBuilder sb = new StringBuilder();
//
//    sb.append("{");
////    sb.append("\n\turlfilter : " + filter);
////    sb.append("\n\turlnormalise : " + normalise);
//    sb.append("\n\n\tpageType : " + pageType);
//    sb.append("\n\turlRegexRule : " + urlRegexRule);
//    if (textFilter != null) {
//      sb.append("\n\n\ttextFilter : " + textFilter.toString());
//    }
//    if (blockFilter != null) {
//      sb.append("\n\n\tblockFilter : " + blockFilter.toString());
//    }
//    sb.append("\n}\n");
//
//    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
//    Configuration conf = NutchConfiguration.create();
//    conf.setBoolean(CRAWL_FILTER_FILTER, false);
//    conf.setBoolean(CRAWL_FILTER_NORMALISE, false);
//
//    String json = FileUtils.readFileToString(new File("/tmp/crawl-filters-test.json"));
//    Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
////    Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.FINAL).create();
//    CrawlFilter crawlFilter = gson.fromJson(json, CrawlFilter.class);
//    crawlFilter.setConf(conf);
//
//    System.out.println(crawlFilter);

    String test =  "com.github:https/about";
    String bound = "com.github:https/\uFFFF";
    System.out.println(test.compareTo(bound));
  }
}
