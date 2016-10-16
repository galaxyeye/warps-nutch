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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.net.RegexURLFilter;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.Objects;

import static org.apache.nutch.util.StringUtil.*;

public class CrawlFilter extends Configured {

  public static final Logger LOG = CrawlFilters.LOG;

  public static PageCategory sniffPageCategory(String url) {
    Objects.requireNonNull(url);

    PageCategory pageCategory = PageCategory.ANY;

    url = url.toLowerCase();

    if (StringUtils.countMatches(url, "/") <= 3) {
      // http://t.tt/12345678
      pageCategory = PageCategory.INDEX;
    }
    else if (DETAIL_PAGE_URL_PATTERN.matcher(url).matches()) {
      pageCategory = PageCategory.DETAIL;
    }
    else if (SEARCH_PAGE_URL_PATTERN.matcher(url).matches()) {
      pageCategory = PageCategory.SEARCH;
    }
    else if (MEDIA_PAGE_URL_PATTERN.matcher(url).matches()) {
      pageCategory = PageCategory.MEDIA;
    }
    else if (INDEX_PAGE_URL_PATTERN.matcher(url).matches()) {
      pageCategory = PageCategory.INDEX;
    }

    return pageCategory;
  }

  public static PageCategory sniffPageCategory(String url, double _char, double _a) {
    PageCategory pageCategory;

    if (_a < 1) {
      _a = 1;
    }

    if (_char/_a < 15) {
      pageCategory = PageCategory.INDEX;
    }
    else if (_char > 1200) {
      pageCategory = PageCategory.DETAIL;
    }
    else {
      return sniffPageCategory(url);
    }

    return pageCategory;
  }

  public enum PageCategory {
    INDEX, DETAIL, SEARCH, MEDIA, ANY
  }

  @Expose
  private PageCategory pageCategory = PageCategory.ANY;
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
      LOG.error(StringUtil.stringifyException(e));
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
    return (pageCategory == PageCategory.DETAIL) && testUrlSatisfied(url);
  }

  public boolean isSearchUrl(String url) {
    return (pageCategory == PageCategory.SEARCH) && testUrlSatisfied(url);
  }

  public boolean isMediaUrl(String url) {
    return (pageCategory == PageCategory.MEDIA) && testUrlSatisfied(url);
  }

  public boolean isIndexUrl(String url) {
    return pageCategory.equals(PageCategory.INDEX) && testUrlSatisfied(url);
  }

  public PageCategory getPageType() {
    return pageCategory;
  }

  public void setPageType(PageCategory pageCategory) {
    this.pageCategory = pageCategory;
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
