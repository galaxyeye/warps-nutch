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
package org.apache.nutch.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.storage.WebPage;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

/**
 * TODO : re-design the table schema to avoid hiding fields in metadata field and to improve efficiency
 * */
public class TableUtil {

  /**
   * Reverses a url's domain. This form is better for storing in hbase. Because
   * scans within the same domain are faster.
   * <p>
   * E.g. "http://bar.foo.com:8983/to/index.html?a=b" becomes
   * "com.foo.bar:8983:http/to/index.html?a=b".
   * 
   * @param urlString
   *          url to be reversed
   * @return Reversed url
   * @throws MalformedURLException
   */
  public static String reverseUrl(String urlString) throws MalformedURLException {
    return reverseUrl(new URL(urlString));
  }

  public static String reverseUrlOrEmpty(String urlString) {
    try {
      return reverseUrl(new URL(urlString));
    } catch (MalformedURLException e) {
      return "";
    }
  }

  /**
   * Reverses a url's domain. This form is better for storing in hbase. Because
   * scans within the same domain are faster.
   * <p>
   * E.g. "http://bar.foo.com:8983/to/index.html?a=b" becomes
   * "com.foo.bar:http:8983/to/index.html?a=b".
   * 
   * @param url
   *          url to be reversed
   * @return Reversed url
   */
  public static String reverseUrl(URL url) {
    String host = url.getHost();
    String file = url.getFile();
    String protocol = url.getProtocol();
    int port = url.getPort();

    StringBuilder buf = new StringBuilder();

    /* reverse host */
    reverseAppendSplits(host, buf);

    /* add protocol */
    buf.append(':');
    buf.append(protocol);

    /* add port if necessary */
    if (port != -1) {
      buf.append(':');
      buf.append(port);
    }

    /* add path */
    if (file.length() > 0 && '/' != file.charAt(0)) {
      buf.append('/');
    }
    buf.append(file);

    return buf.toString();
  }

  public static String unreverseUrl(String reversedUrl) {
    StringBuilder buf = new StringBuilder(reversedUrl.length() + 2);

    int pathBegin = reversedUrl.indexOf('/');
    if (pathBegin == -1)
      pathBegin = reversedUrl.length();
    String sub = reversedUrl.substring(0, pathBegin);

    String[] splits = StringUtils.splitPreserveAllTokens(sub, ':'); // {<reversed
                                                                    // host>,
                                                                    // <port>,
                                                                    // <protocol>}

    buf.append(splits[1]); // add protocol
    buf.append("://");
    reverseAppendSplits(splits[0], buf); // splits[0] is reversed
    // host
    if (splits.length == 3) { // has a port
      buf.append(':');
      buf.append(splits[2]);
    }
    buf.append(reversedUrl.substring(pathBegin));
    return buf.toString();
  }

  /**
   * Given a reversed url, returns the reversed host E.g
   * "com.foo.bar:http:8983/to/index.html?a=b" -> "com.foo.bar"
   * 
   * @param reversedUrl
   *          Reversed url
   * @return Reversed host
   */
  public static String getReversedHost(String reversedUrl) {
    return reversedUrl.substring(0, reversedUrl.indexOf(':'));
  }

  private static void reverseAppendSplits(String string, StringBuilder buf) {
    String[] splits = StringUtils.split(string, '.');
    if (splits.length > 0) {
      for (int i = splits.length - 1; i > 0; i--) {
        buf.append(splits[i]);
        buf.append('.');
      }
      buf.append(splits[0]);
    } else {
      buf.append(string);
    }
  }

  public static String reverseHost(String hostName) {
    StringBuilder buf = new StringBuilder();
    reverseAppendSplits(hostName, buf);
    return buf.toString();

  }

  public static String unreverseHost(String reversedHostName) {
    return reverseHost(reversedHostName); // Reversible
  }

  /**
   * Convert given Utf8 instance to String and and cleans out any offending "�"
   * from the String.
   *
   * TODO : bad function name
   * 
   * @param utf8
   *          Utf8 object
   * @return string-ifed Utf8 object or null if Utf8 instance is null
   */
  public static String toString(CharSequence utf8) {
    return (utf8 == null ? null : StringUtil.cleanField(utf8.toString()));
  }

  public static String toString(CharSequence utf8, String defaultValue) {
    return (utf8 == null ? defaultValue : StringUtil.cleanField(utf8.toString()));
  }

  public static Map<CharSequence, CharSequence> getOutlinks(WebPage page) {
    return page.getOutlinks() == null ? Collections.EMPTY_MAP : page.getOutlinks();
  }
//
//  public static boolean isSeed(WebPage page) {
//    return hasMetadata(page, META_IS_SEED);
//  }
//
//  public static float getFloatMetadata(WebPage page, String key, float defaultValue) {
//    ByteBuffer cashRaw = page.getMetadata().get(new Utf8(key));
//    float value = defaultValue;
//    if (cashRaw != null) {
//      value = Bytes.toFloat(cashRaw.array(), cashRaw.arrayOffset() + cashRaw.position());
//    }
//    return value;
//  }
//
//  public static void setFloatMetadata(WebPage page, String key, float value) {
//    page.getMetadata().put(new Utf8(key), ByteBuffer.wrap(Bytes.toBytes(value)));
//  }
//
//  public static void setTextContentLength(WebPage page, int length) {
//    putMetadata(page, META_TEXT_CONTENT_LENGTH, String.valueOf(length));
//  }
//
//  public static int getTextContentLength(WebPage page) {
//    String ds = getMetadata(page, META_TEXT_CONTENT_LENGTH);
//    return StringUtil.tryParseInt(ds, -1);
//  }
//
//  public static int sniffTextLength(WebPage page) {
//    int length = getTextContentLength(page);
//    if (length >= 0) {
//      return length;
//    }
//
//    Object obj = page.getTemporaryVariable(DOC_FIELD_TEXT_CONTENT_LENGTH);
//    if (obj != null && obj instanceof Integer) {
//      return ((Integer)obj);
//    }
//
//    obj = page.getTemporaryVariable(DOC_FIELD_TEXT_CONTENT);
//    if (obj != null && obj instanceof String) {
//      return ((String)obj).length();
//    }
//
//    CharSequence text = page.getText();
//    if (text != null) {
//      return text.length();
//    }
//
//    return 0;
//  }
//
//  public static float getPageCategoryLikelihood(WebPage page) {
//    return getFloatMetadata(page, META_PAGE_CATEGORY_LIKELIHOOD, 0f);
//  }
//
//  public static void setPageCategoryLikelihood(WebPage page, float likelihood) {
//    setFloatMetadata(page, META_PAGE_CATEGORY_LIKELIHOOD, likelihood);
//  }
//
//  public static boolean isDetailPage(WebPage page, float threshold) {
//    return getPageCategory(page).isDetail() && getPageCategoryLikelihood(page) >= threshold;
//  }
//
//  public static boolean veryLikeDetailPage(WebPage page) {
//    if (page.getBaseUrl() == null) {
//      return false;
//    }
//    boolean detail = CrawlFilter.sniffPageCategoryByUrlPattern(page.getBaseUrl()).isDetail();
//    return detail || isDetailPage(page, 0.85f);
//  }
//
//  public static void setPageCategory(WebPage page, CrawlFilter.PageCategory pageCategory) {
//    putMetadata(page, META_PAGE_CATEGORY, pageCategory.name());
//  }
//
//  public static CrawlFilter.PageCategory getPageCategory(WebPage page) {
//    String pageCategoryStr = getMetadata(page, META_PAGE_CATEGORY);
//
//    try {
//      return CrawlFilter.PageCategory.valueOf(pageCategoryStr);
//    }
//    catch (Throwable e) {
//      return CrawlFilter.PageCategory.UNKNOWN;
//    }
//  }
//
//  public static void setNoFetch(WebPage page) {
//    putMetadata(page, META_NO_FETCH, YES_STRING);
//  }
//
//  public static boolean isNoFetch(WebPage page) {
//    return getMetadata(page, META_NO_FETCH) != null;
//  }
//
//  public static int getDepth(WebPage page) {
//    return getDistance(page);
//  }
//
//  public static int getDistance(WebPage page) {
//    String ds = getMetadata(page, META_DISTANCE);
//    return StringUtil.tryParseInt(ds, MAX_DISTANCE);
//  }
//
//  public static void setDistance(WebPage page, int newDistance) {
//    putMetadata(page, META_DISTANCE, String.valueOf(newDistance));
//  }
//
//  public static void increaseDistance(WebPage page, int distance) {
//    int oldDistance = getDistance(page);
//    if (oldDistance < MAX_DISTANCE - distance) {
//      putMetadata(page, META_DISTANCE, String.valueOf(oldDistance + distance));
//    }
//  }
//
//  public static int calculateFetchPriority(WebPage page) {
//    int depth = getDepth(page);
//    int priority = FETCH_PRIORITY_DEFAULT;
//
//    if (depth < FETCH_PRIORITY_DEPTH_BASE) {
//      priority = FETCH_PRIORITY_DEPTH_BASE - depth;
//    }
//
//    return priority;
//  }
//
//  public static void setFetchPriority(WebPage page, int priority) {
//    TableUtil.putMetadata(page, META_FETCH_PRIORITY, String.valueOf(priority));
//  }
//
//  public static int getFetchPriority(WebPage page, int defaultPriority) {
//    String s = TableUtil.getMetadata(page, META_FETCH_PRIORITY);
//    return StringUtil.tryParseInt(s, defaultPriority);
//  }
//
//  public static void setGenerateTime(WebPage page, long generateTime) {
//    putMetadata(page, PARAM_GENERATE_TIME, String.valueOf(generateTime));
//  }
//
//  public static long getGenerateTime(WebPage page) {
//    String generateTimeStr = getMetadata(page, PARAM_GENERATE_TIME);
//    return StringUtil.tryParseLong(generateTimeStr, -1);
//  }
//
//  public static float getCash(WebPage page) {
//    return getFloatMetadata(page, META_CASH_KEY, 0f);
//  }
//
//  public static void setCash(WebPage page, float cash) {
//    setFloatMetadata(page, META_CASH_KEY, cash);
//  }
//
//  /**
//   * TODO : We need a better solution to track all voted pages
//   * TODO : optimization, consider byte array to track vote history
//   * */
//  public static boolean voteIfAbsent(WebPage voter, WebPage candidate) {
//    String baseUrl = candidate.getBaseUrl().toString();
//    String hash;
//    try {
//      hash = String.valueOf(new URL(baseUrl).hashCode());
//    } catch (MalformedURLException e) {
//      return false;
//    }
//
//    String voteHistory = getVoteHistory(voter);
//    if (!voteHistory.contains(hash)) {
//      if(voteHistory.length() > 0) {
//        voteHistory += ",";
//      }
//
//      voteHistory += hash;
//      if (voteHistory.length() > 2000) {
//        voteHistory = StringUtils.substringAfter(voteHistory, ",");
//      }
//      putMetadata(voter, META_VOTE_HISTORY, voteHistory);
//
//      return true;
//    }
//
//    return false;
//  }
//
//  public static boolean isVoted(WebPage voter, WebPage candidate) {
//    String baseUrl = candidate.getBaseUrl().toString();
//    String hash;
//    try {
//      hash = String.valueOf(new URL(baseUrl).hashCode());
//    } catch (MalformedURLException e) {
//      return false;
//    }
//
//    String voteHistory = getVoteHistory(voter);
//    return voteHistory.contains(hash);
//  }
//
//  public static String getVoteHistory(WebPage voter) {
//    return getMetadata(voter, META_VOTE_HISTORY, "");
//  }
//
//  public static void setPublishTime(WebPage page, String publishTime) {
//    putMetadata(page, META_PUBLISH_TIME, publishTime);
//  }
//
//  public static void setPublishTime(WebPage page, Date publishTime) {
//    putMetadata(page, META_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
//  }
//
//  public static String getPublishTimeStr(WebPage page) {
//    return getMetadata(page, META_PUBLISH_TIME);
//  }
//
//  public static Instant getPublishTime(WebPage page) {
//    String publishTimeStr = getPublishTimeStr(page);
//    return DateTimeUtil.parseTime(publishTimeStr);
//  }
//
//  public static String getReferrer(WebPage page) {
//    return getMetadata(page, META_REFERRER);
//  }
//
//  public static void setReferrer(WebPage page, String referrer) {
//    putMetadata(page, META_REFERRER, referrer);
//  }
//
//  public static int getFetchCount(WebPage page) {
//    String referredPages = getMetadata(page, META_FETCH_TIMES);
//    return StringUtil.tryParseInt(referredPages, 0);
//  }
//
//  public static void setFetchCount(WebPage page, int count) {
//    putMetadata(page, META_FETCH_TIMES, String.valueOf(count));
//  }
//
//  public static void increaseFetchCount(WebPage page) {
//    int count = getFetchCount(page);
//    putMetadata(page, META_FETCH_TIMES, String.valueOf(count + 1));
//  }
//
//  public static long getReferredArticles(WebPage page) {
//    String referredPages = getMetadata(page, META_REFERRED_ARTICLES);
//    return StringUtil.tryParseLong(referredPages, 0);
//  }
//
//  public static void setReferredArticles(WebPage page, long count) {
//    putMetadata(page, META_REFERRED_ARTICLES, String.valueOf(count));
//  }
//
//  public static void increaseReferredArticles(WebPage page, long count) {
//    long oldCount = getReferredArticles(page);
//    putMetadata(page, META_REFERRED_ARTICLES, String.valueOf(oldCount + count));
//  }
//
//  public static long getTotalOutLinkCount(WebPage page) {
//    String outLinks = getMetadata(page, META_OUT_LINK_COUNT);
//    return StringUtil.tryParseLong(outLinks, 0);
//  }
//
//  public static void setTotalOutLinkCount(WebPage page, long count) {
//    putMetadata(page, META_OUT_LINK_COUNT, String.valueOf(count));
//  }
//
//  public static void increaseTotalOutLinkCount(WebPage page, long count) {
//    long oldCount = getTotalOutLinkCount(page);
//    putMetadata(page, META_OUT_LINK_COUNT, String.valueOf(oldCount + count));
//  }
//
//  public static int sniffOutLinkCount(WebPage page) {
//    int _a = page.getOutlinks().size();
//    if (_a == 0) {
//      Object count = page.getTemporaryVariable(VAR_OUTLINKS_COUNT);
//      if (count != null && count instanceof Integer) {
//        _a = (int)count;
//      }
//    }
//
//    return _a;
//  }
//
//  public static long getReferredChars(WebPage page) {
//    String referredPages = getMetadata(page, META_REFERRED_CHARS);
//    return StringUtil.tryParseLong(referredPages, 0);
//  }
//
//  public static void setReferredChars(WebPage page, long count) {
//    putMetadata(page, META_REFERRED_CHARS, String.valueOf(count));
//  }
//
//  public static void increaseReferredChars(WebPage page, long count) {
//    long oldCount = getReferredChars(page);
//    setReferredChars(page, oldCount + count);
//  }
//
//  public static Instant getReferredPublishTime(WebPage page) {
//    String publishTimeStr = getMetadata(page, META_REFERRED_PUBLISH_TIME);
//    return DateTimeUtil.parseTime(publishTimeStr);
//  }
//
//  public static void setReferredPublishTime(WebPage page, Instant publishTime) {
//    putMetadata(page, META_REFERRED_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
//  }
//
//  public static boolean updateReferredPublishTime(WebPage page, Instant newPublishTime) {
//    Instant latestTime = getReferredPublishTime(page);
//    if (newPublishTime.isAfter(latestTime)) {
//      setReferredPublishTime(page, newPublishTime);
//      return true;
//    }
//
//    return false;
//  }
//
//  public static Instant getFetchTime(WebPage page) {
//    return Instant.ofEpochMilli(page.getFetchTime());
//  }
//
//  public static Duration getFetchInterval(WebPage page) {
//    return Duration.ofSeconds(page.getFetchInterval());
//  }
//
//  public static Instant getHeaderLastModifiedTime(WebPage page, Instant defaultValue) {
//    CharSequence lastModified = page.getHeaders().get(new Utf8(HttpHeaders.LAST_MODIFIED));
//    if (lastModified != null) {
//      return DateTimeUtil.parseTime(lastModified.toString());
//    }
//
//    return defaultValue;
//  }
//
//  public static String getFetchTimeHistory(WebPage page, String defaultValue) {
//    String s = TableUtil.getMetadata(page, Metadata.META_FETCH_TIME_HISTORY);
//    return s == null ? defaultValue : s;
//  }
//
//  public static void putFetchTimeHistory(WebPage page, long fetchTime) {
//    String fetchTimeHistory = TableUtil.getMetadata(page, Metadata.META_FETCH_TIME_HISTORY);
//    fetchTimeHistory = DateTimeUtil.constructTimeHistory(fetchTimeHistory, fetchTime, 10);
//    TableUtil.putMetadata(page, Metadata.META_FETCH_TIME_HISTORY, fetchTimeHistory);
//  }
//
//  public static Instant getFirstCrawlTime(WebPage page, Instant defaultValue) {
//    Instant firstCrawlTime = null;
//
//    String fetchTimeHistory = TableUtil.getFetchTimeHistory(page, "");
//    if (!fetchTimeHistory.isEmpty()) {
//      String[] times = fetchTimeHistory.split(",");
//      Instant time = DateTimeUtil.parseTime(times[0]);
//      if (time.isAfter(Instant.EPOCH)) {
//        firstCrawlTime = time;
//      }
//    }
//
//    return firstCrawlTime == null ? defaultValue : firstCrawlTime;
//  }
//
//  public static String getIndexTimeHistory(WebPage page, String defaultValue) {
//    String s = TableUtil.getMetadata(page, Metadata.META_INDEX_TIME_HISTORY);
//    return s == null ? defaultValue : s;
//  }
//
//  /**
//   * TODO : consider hbase's record history feature
//   * */
//  public static void putIndexTimeHistory(WebPage page, long indexTime) {
//    String indexTimeHistory = TableUtil.getMetadata(page, Metadata.META_INDEX_TIME_HISTORY);
//    indexTimeHistory = DateTimeUtil.constructTimeHistory(indexTimeHistory, indexTime, 10);
//    TableUtil.putMetadata(page, Metadata.META_INDEX_TIME_HISTORY, indexTimeHistory);
//  }
//
//  public static Instant getFirstIndexTime(WebPage page, Instant defaultValue) {
//    Instant firstIndexTime = null;
//
//    String indexTimeHistory = TableUtil.getIndexTimeHistory(page, "");
//    if (!indexTimeHistory.isEmpty()) {
//      String[] times = indexTimeHistory.split(",");
//      Instant time = DateTimeUtil.parseTime(times[0]);
//      if (time.isAfter(Instant.EPOCH)) {
//        firstIndexTime = time;
//      }
//    }
//
//    return firstIndexTime == null ? defaultValue : firstIndexTime;
//  }
//
//  public static void putMark(WebPage page, CharSequence key, CharSequence value) {
//    page.getMarkers().put(key, value);
//  }
//
//  public static CharSequence getMark(WebPage page, CharSequence key) {
//    return page.getMarkers().get(key);
//  }
//
//  public static String getHeader(WebPage page, String name, String defaultValue) {
//    CharSequence value = page.getHeaders().get(new Utf8(name));
//    return value == null ? defaultValue : value.toString();
//  }
//
//  public static void putHeader(WebPage page, String name, String value) {
//    page.getHeaders().put(new Utf8(name), new Utf8(value));
//  }
//
//  /**
//   * TODO : Why the nutch team wrap the key using Utf8?
//   * */
//  public static void putAllMetadata(WebPage page, Map<String, String> metadata) {
//    for (Map.Entry<String, String> entry : metadata.entrySet()) {
//      String k = entry.getKey();
//      String v = entry.getValue();
//
//      putMetadata(page, k, v);
//    }
//  }
//
//  public static void putMetadata(WebPage page, String key, String value) {
//    page.getMetadata().put(new Utf8(key), value == null ? null : ByteBuffer.wrap(value.getBytes()));
//  }
//
//  public static void putMetadata(WebPage page, Utf8 key, ByteBuffer value) {
//    page.getMetadata().put(key, value);
//  }
//
//  public static String getMetadata(WebPage page, String key) {
//    ByteBuffer bvalue = page.getMetadata().get(new Utf8(key));
//    if (bvalue == null) {
//      return null;
//    }
//    return Bytes.toString(bvalue.array());
//  }
//
//  public static String getMetadata(WebPage page, String key, String defaultValue) {
//    String value = getMetadata(page, key);
//    return value == null ? defaultValue : value;
//  }
//
//  public static boolean hasMetadata(WebPage page, String key) {
//    ByteBuffer bvalue = page.getMetadata().get(new Utf8(key));
//    return bvalue != null;
//  }
//
//  // But only delete when they exist. This is much faster for the underlying store
//  // The markers are on the input anyway.
//  public static void clearMetadata(WebPage page, Utf8 key) {
//    if (page.getMetadata().get(key) != null) {
//      page.getMetadata().put(key, null);
//    }
//  }
//
//  public static void clearMetadata(WebPage page, String key) {
//    if (getMetadata(page, key) != null) {
//      putMetadata(page, key, null);
//    }
//  }
//
//  // But only delete when they exist. This is much faster for the underlying store
//  // The markers are on the input anyway.
//  public static void clearTmpMetadata(WebPage page) {
//    page.getMetadata().keySet().stream()
//        .filter(key -> key.toString().startsWith(Metadata.META_TMP))
//        .forEach(key -> page.getMetadata().put(key, null));
//  }
//
//  public static Map<String, String> getMetadata(WebPage page) {
//    Map<String, String> result = Maps.newHashMap();
//
//    for (CharSequence key : page.getMetadata().keySet()) {
//      ByteBuffer bvalues = page.getMetadata().get(key);
//      if (bvalues != null) {
//        String value = Bytes.toString(bvalues.array());
//        result.put(key.toString(), value);
//
//        // String[] values = value.split("\t");
//        // System.out.println(key + " : " + value);
//      }
//    }
//
//    return result;
//  }
}
