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

import com.google.common.collect.Maps;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.WebPage;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Date;
import java.util.Map;

import static org.apache.nutch.metadata.Metadata.*;

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

  public static boolean isSeed(WebPage page) {
    return hasMetadata(page, META_IS_SEED);
  }

  public static int getDepth(WebPage page) {
    return getDistance(page);
  }

  public static int getDistance(WebPage page) {
    String ds = getMetadata(page, META_DISTANCE);
    return StringUtil.tryParseInt(ds, MAX_DISTANCE);
  }

  public static void setDistance(WebPage page, int newDistance) {
    putMetadata(page, META_DISTANCE, String.valueOf(newDistance));
  }

  public static int calculateFetchPriority(WebPage page) {
    int depth = getDepth(page);
    int priority = FETCH_PRIORITY_DEFAULT;

    if (depth < FETCH_PRIORITY_DEPTH_BASE) {
      priority = FETCH_PRIORITY_DEPTH_BASE - depth;
    }

    setFetchPriorityIfAbsent(page, priority);

    return priority;
  }

  /**
   * TODO : use a standalone field for page
   * */
  public static void setFetchPriority(WebPage page, int priority) {
    TableUtil.putMetadata(page, META_FETCH_PRIORITY, String.valueOf(priority));
  }

  public static void setFetchPriorityIfAbsent(WebPage page, int priority) {
    String s = TableUtil.getMetadata(page, META_FETCH_PRIORITY);
    if (s == null) {
      setFetchPriority(page, priority);
    }
  }

  public static int getFetchPriority(WebPage page, int defaultPriority) {
    String s = TableUtil.getMetadata(page, META_FETCH_PRIORITY);
    return StringUtil.tryParseInt(s, defaultPriority);
  }

  public static void setGenerateTime(WebPage page, long generateTime) {
    putMetadata(page, PARAM_GENERATE_TIME, String.valueOf(generateTime));
  }

  public static long getGenerateTime(WebPage page) {
    String generateTimeStr = getMetadata(page, PARAM_GENERATE_TIME);
    return StringUtil.tryParseLong(generateTimeStr, -1);
  }

  public static float getCash(WebPage page) {
    ByteBuffer cashRaw = page.getMetadata().get(new Utf8(META_CASH_KEY));
    float cash = 0.0f;
    if (cashRaw != null) {
      cash = Bytes.toFloat(cashRaw.array(), cashRaw.arrayOffset() + cashRaw.position());
    }
    return cash;
  }

  public static void setCash(WebPage page, float cash) {
    page.getMetadata().put(new Utf8(META_CASH_KEY), ByteBuffer.wrap(Bytes.toBytes(cash)));
  }

  public static void setPublishTime(WebPage page, String publishTime) {
    putMetadata(page, META_PUBLISH_TIME, publishTime);
  }

  public static void setPublishTime(WebPage page, Date publishTime) {
    putMetadata(page, META_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
  }

  public static String getPublishTimeStr(WebPage page) {
    return getMetadata(page, META_PUBLISH_TIME);
  }

  public static long getPublishTime(WebPage page) {
    String publishTimeStr = getPublishTimeStr(page);
    return DateTimeUtil.parseTime(publishTimeStr);
  }

  public static long getReferredPageCount(WebPage page) {
    String referredPages = getMetadata(page, META_REFERRED_PAGES);
    return StringUtil.tryParseLong(referredPages, 0);
  }

  public static void setReferredPageCount(WebPage page, long count) {
    putMetadata(page, META_REFERRED_PAGES, String.valueOf(count));
  }

  public static void increaseReferredPageCount(WebPage page, long count) {
    long oldCount = getReferredPageCount(page);
    putMetadata(page, META_REFERRED_PAGES, String.valueOf(oldCount + count));
  }

  public static long getLatestReferredPublishTime(WebPage page) {
    String publishTimeStr = getMetadata(page, META_LATEST_REFERRED_ARTICLE_PUBLISH_TIME);
    return DateTimeUtil.parseTime(publishTimeStr);
  }

  public static void setLatestReferredPublishTime(WebPage page, long publishTime) {
    putMetadata(page, META_LATEST_REFERRED_ARTICLE_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
  }

  public static boolean updateLatestReferredPublishTime(WebPage page, long newPublishTime) {
    if (newPublishTime <= Duration.ofDays(-365 * 30).toMillis()) {
      NutchUtil.LOG.warn("Publish time out of range for page " + page.getBaseUrl());
      return false;
    }

    long latestTime = getLatestReferredPublishTime(page);
    if (latestTime < newPublishTime) {
      setLatestReferredPublishTime(page, newPublishTime);
      return true;
    }

    return false;
  }

  public static String getFetchTimeHistory(WebPage page, String defaultValue) {
    String s = TableUtil.getMetadata(page, Metadata.META_FETCH_TIME_HISTORY);
    return s == null ? defaultValue : s;
  }

  public static void putFetchTimeHistory(WebPage page, long fetchTime) {
    String fetchTimeHistory = TableUtil.getMetadata(page, Metadata.META_FETCH_TIME_HISTORY);
    fetchTimeHistory = DateTimeUtil.constructTimeHistory(fetchTimeHistory, fetchTime, 10);
    TableUtil.putMetadata(page, Metadata.META_FETCH_TIME_HISTORY, fetchTimeHistory);
  }

  public static Date getFirstCrawlTime(WebPage page, Date defaultValue) {
    Date firstCrawlTime = null;

    String fetchTimeHistory = TableUtil.getFetchTimeHistory(page, "");
    if (!fetchTimeHistory.isEmpty()) {
      String[] times = fetchTimeHistory.split(",");
      long time = DateTimeUtil.parseTime(times[0]);
      if (time > 0) {
        firstCrawlTime = new Date(time);
      }
    }

    return firstCrawlTime == null ? defaultValue : firstCrawlTime;
  }

  public static String getIndexTimeHistory(WebPage page, String defaultValue) {
    String s = TableUtil.getMetadata(page, Metadata.META_INDEX_TIME_HISTORY);
    return s == null ? defaultValue : s;
  }

  public static void putIndexTimeHistory(WebPage page, long indexTime) {
    String indexTimeHistory = TableUtil.getMetadata(page, Metadata.META_INDEX_TIME_HISTORY);
    indexTimeHistory = DateTimeUtil.constructTimeHistory(indexTimeHistory, indexTime, 10);
    TableUtil.putMetadata(page, Metadata.META_INDEX_TIME_HISTORY, indexTimeHistory);
  }

  public static Date getFirstIndexTime(WebPage page, Date defaultValue) {
    Date firstIndexTime = null;

    String indexTimeHistory = TableUtil.getIndexTimeHistory(page, "");
    if (!indexTimeHistory.isEmpty()) {
      String[] times = indexTimeHistory.split(",");
      long time = DateTimeUtil.parseTime(times[0]);
      if (time > 0) {
        firstIndexTime = new Date(time);
      }
    }

    return firstIndexTime == null ? defaultValue : firstIndexTime;
  }

  public static void putMark(WebPage page, CharSequence key, CharSequence value) {
    page.getMarkers().put(key, value);
  }

  public static CharSequence getMark(WebPage page, CharSequence key) {
    return page.getMarkers().get(key);
  }

  public static void putMetadata(WebPage page, Utf8 key, ByteBuffer value) {
    page.getMetadata().put(key, value);
  }

  /**
   * TODO : Why the nutch team wrap the key using Utf8?
   * */
  public static void putAllMetadata(WebPage page, Map<String, String> metadata) {
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String k = entry.getKey();
      String v = entry.getValue();

      putMetadata(page, k, v);
    }
  }

  public static void putMetadata(WebPage page, String key, String value) {
    page.getMetadata().put(new Utf8(key), value == null ? null : ByteBuffer.wrap(value.getBytes()));
  }

  public static String getMetadata(WebPage page, String key) {
    ByteBuffer bvalue = page.getMetadata().get(new Utf8(key));
    if (bvalue == null) {
      return null;
    }
    return Bytes.toString(bvalue.array());
  }

  public static String getMetadata(WebPage page, String key, String defaultValue) {
    String value = getMetadata(page, key);
    return value == null ? defaultValue : value;
  }

  public static boolean hasMetadata(WebPage page, String key) {
    ByteBuffer bvalue = page.getMetadata().get(new Utf8(key));
    return bvalue != null;
  }

  // But only delete when they exist. This is much faster for the underlying store
  // The markers are on the input anyway.
  public static void clearMetadata(WebPage page, Utf8 key) {
    if (page.getMetadata().get(key) != null) {
      page.getMetadata().put(key, null);
    }
  }

  public static void clearMetadata(WebPage page, String key) {
    if (getMetadata(page, key) != null) {
      putMetadata(page, key, null);
    }
  }

  // But only delete when they exist. This is much faster for the underlying store
  // The markers are on the input anyway.
  public static void clearTmpMetadata(WebPage page) {
    page.getMetadata().keySet().stream()
        .filter(key -> key.toString().startsWith(Metadata.META_TMP))
        .forEach(key -> page.getMetadata().put(key, null));
  }

  public static Map<String, String> getMetadata(WebPage page) {
    Map<String, String> result = Maps.newHashMap();

    for (CharSequence key : page.getMetadata().keySet()) {
      ByteBuffer bvalues = page.getMetadata().get(key);
      if (bvalues != null) {
        String value = Bytes.toString(bvalues.array());
        result.put(key.toString(), value);

        // String[] values = value.split("\t");
        // System.out.println(key + " : " + value);
      }
    }

    return result;
  }
}
