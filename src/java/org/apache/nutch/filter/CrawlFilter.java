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
package org.apache.nutch.filter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.nutch.metadata.Nutch.DOC_FIELD_TEXT_CONTENT;

public class CrawlFilter extends Configured {

  public static final Logger LOG = CrawlFilters.LOG;

  /**
   * The follow patterns are simple rule to indicate a url's category, this is a very simple solution, and the result is
   * not accurate
   * TODO : configurable
   * */
  public static Pattern[] INDEX_PAGE_URL_PATTERNS = {
      Pattern.compile(".+tieba.baidu.com/.+search.+"),
      Pattern.compile(".+(index|list|tags|chanel).+"),
  };

  public static Pattern SEARCH_PAGE_URL_PATTERN = Pattern.compile(".+(search|query|select).+");

  // TODO : configurable, testable
  public static Pattern[] DETAIL_PAGE_URL_PATTERNS = {
      Pattern.compile(".+tieba.baidu.com/p/(\\d+)"),
      Pattern.compile(".+(detail|item|article|book|good|product|thread|view|post|content|/20[012][0-9]/{0,1}[01][0-9]/|/20[012]-[0-9]{0,1}-[01][0-9]/|/\\d{2,}/\\d{5,}|\\d{7,}).+")
  };

  public static Pattern MEDIA_PAGE_URL_PATTERN = Pattern.compile(".+(pic|picture|photo|avatar|photoshow|video).+");

  /** TODO : need carefully test */
  public static CrawlFilter.PageCategory sniffPageCategory(WebPage page) {
    if (page.getBaseUrl() == null) {
      return CrawlFilter.PageCategory.UNKNOWN;
    }

    String url = page.getBaseUrl().toString();
    CrawlFilter.PageCategory pageCategory = sniffPageCategoryByUrlPattern(url);
    if (pageCategory.isDetail()) {
      return pageCategory;
    }

    String textContent = (String)page.getTemporaryVariable(DOC_FIELD_TEXT_CONTENT);
    if (textContent == null) {
      return pageCategory;
    }

    double _char = TableUtil.sniffTextLength(page);
    double _a = page.getOutlinks().size();
    if (_a == 0) {
      Object count = page.getTemporaryVariable("outlinks_count");
      if (count != null && count instanceof Integer) {
        _a = (int)count;
      }
    }

    if (_char < 100) {
      if (_a > 30) {
        pageCategory = PageCategory.INDEX;
      }
    }
    else {
      return sniffPageCategory(url, _char, _a);
    }

    return pageCategory;
  }

  public static PageCategory sniffPageCategory(String url, double _char, double _a) {
    PageCategory pageCategory = sniffPageCategoryByTextDensity(_char, _a);

    return pageCategory == PageCategory.UNKNOWN ? sniffPageCategoryByUrlPattern(url) : pageCategory;
  }

  /* TODO : use machine learning to calculate the parameters */
  public static PageCategory sniffPageCategoryByTextDensity(double _char, double _a) {
    PageCategory pageCategory = PageCategory.UNKNOWN;

    if (_a < 1) {
      _a = 1;
    }

    if (_a > 60 && _char/_a < 20) {
      // 索引页：链接数不少于60个，文本密度小于20
      pageCategory = PageCategory.INDEX;
    }
    else if (_char/_a > 30) {
      pageCategory = PageCategory.DETAIL;
    }

    return pageCategory;
  }

  /**
   * A simple regex rule to sniff the possible category of a web page
   * */
  public static PageCategory sniffPageCategoryByUrlPattern(String urlString) {
    Objects.requireNonNull(urlString);

    PageCategory pageCategory = PageCategory.UNKNOWN;

    final String url = urlString.toLowerCase();

    // Notice : ***DO KEEP*** the right order
    if (url.endsWith("/")) {
      pageCategory = PageCategory.INDEX;
    }
    else if (StringUtils.countMatches(url, "/") <= 3) {
      // http://t.tt/12345678
      pageCategory = PageCategory.INDEX;
    }
    else if (Stream.of(INDEX_PAGE_URL_PATTERNS).anyMatch(pattern -> pattern.matcher(url).matches())) {
      pageCategory = PageCategory.INDEX;
    }
    else if (Stream.of(DETAIL_PAGE_URL_PATTERNS).anyMatch(pattern -> pattern.matcher(url).matches())){
      pageCategory = PageCategory.DETAIL;
    }
    else if (SEARCH_PAGE_URL_PATTERN.matcher(url).matches()) {
      pageCategory = PageCategory.SEARCH;
    }
    else if (MEDIA_PAGE_URL_PATTERN.matcher(url).matches()) {
      pageCategory = PageCategory.MEDIA;
    }

    return pageCategory;
  }

  public enum PageCategory {
    INDEX, DETAIL, SEARCH, MEDIA, UNKNOWN;

    public boolean isIndex() {
      return this == INDEX;
    }

    public boolean isDetail() {
      return this == DETAIL;
    }

    public boolean isAny() { return this == UNKNOWN; }
  }

  @Expose
  private PageCategory pageCategory = PageCategory.UNKNOWN;
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
