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
package org.apache.nutch.storage;

import com.google.common.collect.Maps;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.filter.CrawlFilter;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static org.apache.nutch.metadata.Metadata.*;
import static org.apache.nutch.metadata.Nutch.DOC_FIELD_TEXT_CONTENT;
import static org.apache.nutch.metadata.Nutch.DOC_FIELD_TEXT_CONTENT_LENGTH;

/**
 * TODO : re-design the table schema to avoid hiding fields in metadata field and to improve efficiency
 * */
public class WrappedWebPage {

  private WebPage page;

  public WrappedWebPage(WebPage page) {
    this.page = page;
  }

  public static WrappedWebPage newWebPage() {
    return new WrappedWebPage(WebPage.newBuilder().build());
  }

  public static WrappedWebPage wrap(WebPage page) { return new WrappedWebPage(page); }

  public WebPage get() {
    return page;
  }

  public CharSequence getBaseUrl() {
    return page.getBaseUrl();
  }

  public void setBaseUrl(CharSequence value) {
    page.setBaseUrl(value);
  }

  public Integer getStatus() {
    return page.getStatus();
  }

  public void setStatus(Integer value) {
    page.setStatus(value);
  }

  public void setFetchTime(Instant time) {
    page.setFetchTime(time.toEpochMilli());
  }

  public Instant getPrevFetchTime() {
    return Instant.ofEpochMilli(page.getPrevFetchTime());
  }

  public void setPrevFetchTime(Instant time) {
    page.setPrevFetchTime(time.toEpochMilli());
  }

  public void setFetchInterval(long interval) {
    page.setFetchInterval((int)interval);
  }

  public void setFetchInterval(Duration interval) {
    page.setFetchInterval((int)interval.getSeconds());
  }

  public Integer getRetriesSinceFetch() {
    return page.getRetriesSinceFetch();
  }

  public void setRetriesSinceFetch(Integer value) {
    page.setRetriesSinceFetch(value);
  }

  public Instant getModifiedTime() {
    return Instant.ofEpochMilli(page.getModifiedTime());
  }

  public void setModifiedTime(Instant value) {
    page.setModifiedTime(value.toEpochMilli());
  }

  public Instant getPrevModifiedTime() {
    return Instant.ofEpochMilli(page.getPrevModifiedTime());
  }

  public void setPrevModifiedTime(Instant value) {
    page.setPrevModifiedTime(value.toEpochMilli());
  }

  public ProtocolStatus getProtocolStatus() {
    return page.getProtocolStatus();
  }

  public void setProtocolStatus(ProtocolStatus value) {
    page.setProtocolStatus(value);
  }

  /**
   * The entire raw document content e.g. raw XHTML
   * */
  public ByteBuffer getContent() {
    return page.getContent();
  }

  public void setContent(ByteBuffer value) {
    page.setContent(value);
  }

  public CharSequence getContentType() {
    return page.getContentType();
  }

  public void setContentType(CharSequence value) {
    page.setContentType(value);
  }

  public ByteBuffer getPrevSignature() {
    return page.getPrevSignature();
  }

  public void setPrevSignature(ByteBuffer value) {
    page.setPrevSignature(value);
  }

  /**
   * An implementation of a WebPage's signature from which it can be identified and referenced at any point in time.
   * This is essentially the WebPage's fingerprint represnting its state for any point in time.
   * */
  public ByteBuffer getSignature() {
    return page.getSignature();
  }

  public void setSignature(ByteBuffer value) {
    page.setSignature(value);
  }

  public CharSequence getTitle() {
    return page.getTitle();
  }

  public void setTitle(CharSequence value) {
    page.setTitle(value);
  }

  public CharSequence getText() {
    return page.getText();
  }

  public void setText(CharSequence value) {
    page.setText(value);
  }

  public ParseStatus getParseStatus() {
    return page.getParseStatus();
  }

  public void setParseStatus(ParseStatus value) {
    page.setParseStatus(value);
  }

  public Float getScore() {
    return page.getScore();
  }

  public void setScore(Float value) {
    page.setScore(value);
  }

  public CharSequence getReprUrl() {
    return page.getReprUrl();
  }

  public void setReprUrl(CharSequence value) {
    page.setReprUrl(value);
  }

  /**
   * Header information returned from the web server used to server the content which is subsequently fetched from.
   * This includes keys such as TRANSFER_ENCODING, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_LOCATION, CONTENT_DISPOSITION, CONTENT_MD5, CONTENT_TYPE, LAST_MODIFIED and LOCATION.
   * */
  public Map<CharSequence,CharSequence> getHeaders() {
    return page.getHeaders();
  }

  /**
   * Header information returned from the web server used to server the content which is subsequently fetched from.
   * This includes keys such as TRANSFER_ENCODING, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_LOCATION, CONTENT_DISPOSITION, CONTENT_MD5, CONTENT_TYPE, LAST_MODIFIED and LOCATION.
   * * @param value the value to set.
   */
  public void setHeaders(Map<CharSequence,CharSequence> value) {
    page.setHeaders(value);
  }

  /**
   * Embedded hyperlinks which direct outside of the current domain.   */
  public Map<CharSequence,CharSequence> getOutlinks() {
    return page.getOutlinks();
  }

  /**
   * Embedded hyperlinks which direct outside of the current domain.   * @param value the value to set.
   */
  public void setOutlinks(Map<CharSequence,CharSequence> value) {
    page.setOutlinks(value);
  }

  public Map<CharSequence,CharSequence> getInlinks() {
    return page.getInlinks();
  }

  public void setInlinks(Map<CharSequence,CharSequence> value) {
    page.setInlinks(value);
  }

  public Map<CharSequence,CharSequence> getMarkers() {
    return page.getMarkers();
  }

  public void setMarkers(Map<CharSequence,CharSequence> value) {
    page.setMarkers(value);
  }

  public CharSequence getBatchId() {
    return page.getBatchId();
  }

  public void setBatchId(CharSequence value) {
    page.setBatchId(value);
  }

  public Map<String, Object> getProgramVariables() { return page.getProgramVariables(); }

  public Object getTemporaryVariable(String name) { return getProgramVariables().get(name); }

  public String getTemporaryVariableAsString(String name) {
    Object value = getProgramVariables().get(name);
    return value == null ? null : value.toString();
  }

  public String getTemporaryVariableAsString(String name, String defaultValue) {
    Object value = getProgramVariables().get(name);
    return value == null ? defaultValue : value.toString();
  }

  public void setTmporaryVariable(String name, Object value) { getProgramVariables().put(name, value); }

  /*******************************************************************************************************/
  /*
  /*******************************************************************************************************/
  public boolean isSeed() {
    return hasMetadata(META_IS_SEED);
  }

  public float getFloatMetadata(String key, float defaultValue) {
    ByteBuffer cashRaw = page.getMetadata().get(new Utf8(key));
    float value = defaultValue;
    if (cashRaw != null) {
      value = Bytes.toFloat(cashRaw.array(), cashRaw.arrayOffset() + cashRaw.position());
    }
    return value;
  }

  public void setFloatMetadata(String key, float value) {
    page.getMetadata().put(new Utf8(key), ByteBuffer.wrap(Bytes.toBytes(value)));
  }

  public void setTextContentLength(int length) {
    putMetadata(META_TEXT_CONTENT_LENGTH, String.valueOf(length));
  }

  public int getTextContentLength() {
    String ds = getMetadata(META_TEXT_CONTENT_LENGTH);
    return StringUtil.tryParseInt(ds, -1);
  }

  public int sniffTextLength() {
    int length = getTextContentLength();
    if (length >= 0) {
      return length;
    }

    Object obj = page.getTemporaryVariable(DOC_FIELD_TEXT_CONTENT_LENGTH);
    if (obj != null && obj instanceof Integer) {
      return ((Integer)obj);
    }

    obj = page.getTemporaryVariable(DOC_FIELD_TEXT_CONTENT);
    if (obj != null && obj instanceof String) {
      return ((String)obj).length();
    }

    CharSequence text = page.getText();
    if (text != null) {
      return text.length();
    }

    return 0;
  }

  public float getPageCategoryLikelihood() {
    return getFloatMetadata(META_PAGE_CATEGORY_LIKELIHOOD, 0f);
  }

  public void setPageCategoryLikelihood(float likelihood) {
    setFloatMetadata(META_PAGE_CATEGORY_LIKELIHOOD, likelihood);
  }

  public boolean isDetailPage(float threshold) {
    return getPageCategory().isDetail() && getPageCategoryLikelihood() >= threshold;
  }

  public boolean veryLikeDetailPage() {
    if (page.getBaseUrl() == null) {
      return false;
    }
    boolean detail = CrawlFilter.sniffPageCategoryByUrlPattern(page.getBaseUrl()).isDetail();
    return detail || isDetailPage(0.85f);
  }

  public void setPageCategory(CrawlFilter.PageCategory pageCategory) {
    putMetadata(META_PAGE_CATEGORY, pageCategory.name());
  }

  public CrawlFilter.PageCategory getPageCategory() {
    String pageCategoryStr = getMetadata(META_PAGE_CATEGORY);

    try {
      return CrawlFilter.PageCategory.valueOf(pageCategoryStr);
    }
    catch (Throwable e) {
      return CrawlFilter.PageCategory.UNKNOWN;
    }
  }

  public void setNoFetch() {
    putMetadata(META_NO_FETCH, YES_STRING);
  }

  public boolean isNoFetch() {
    return getMetadata(META_NO_FETCH) != null;
  }

  public int getDepth() {
    return getDistance();
  }

  public int getDistance() {
    String ds = getMetadata(META_DISTANCE);
    return StringUtil.tryParseInt(ds, MAX_DISTANCE);
  }

  public void setDistance(int newDistance) {
    putMetadata(META_DISTANCE, String.valueOf(newDistance));
  }

  public void increaseDistance(int distance) {
    int oldDistance = getDistance();
    if (oldDistance < MAX_DISTANCE - distance) {
      putMetadata(META_DISTANCE, String.valueOf(oldDistance + distance));
    }
  }

  public int calculateFetchPriority() {
    int depth = getDepth();
    int priority = FETCH_PRIORITY_DEFAULT;

    if (depth < FETCH_PRIORITY_DEPTH_BASE) {
      priority = FETCH_PRIORITY_DEPTH_BASE - depth;
    }

    return priority;
  }

  public void setFetchPriority(int priority) {
    putMetadata(META_FETCH_PRIORITY, String.valueOf(priority));
  }

  public int getFetchPriority(int defaultPriority) {
    String s = getMetadata(META_FETCH_PRIORITY);
    return StringUtil.tryParseInt(s, defaultPriority);
  }

  public void setGenerateTime(long generateTime) {
    putMetadata(PARAM_GENERATE_TIME, String.valueOf(generateTime));
  }

  public long getGenerateTime() {
    String generateTimeStr = getMetadata(PARAM_GENERATE_TIME);
    return StringUtil.tryParseLong(generateTimeStr, -1);
  }

  public float getCash() {
    return getFloatMetadata(META_CASH_KEY, 0f);
  }

  public void setCash(float cash) {
    setFloatMetadata(META_CASH_KEY, cash);
  }

  /**
   * TODO : We need a better solution to track all voted pages
   * TODO : optimization, consider byte array to track vote history
   * */
  public boolean voteIfAbsent(WrappedWebPage candidate) {
    String baseUrl = candidate.getBaseUrl().toString();
    String hash;
    try {
      hash = String.valueOf(new URL(baseUrl).hashCode());
    } catch (MalformedURLException e) {
      return false;
    }

    String voteHistory = getVoteHistory();
    if (!voteHistory.contains(hash)) {
      if(voteHistory.length() > 0) {
        voteHistory += ",";
      }

      voteHistory += hash;
      if (voteHistory.length() > 2000) {
        voteHistory = StringUtils.substringAfter(voteHistory, ",");
      }
      putMetadata(META_VOTE_HISTORY, voteHistory);

      return true;
    }

    return false;
  }

  public boolean isVoted(WrappedWebPage candidate) {
    String baseUrl = candidate.getBaseUrl().toString();
    String hash;
    try {
      hash = String.valueOf(new URL(baseUrl).hashCode());
    } catch (MalformedURLException e) {
      return false;
    }

    String voteHistory = getVoteHistory();
    return voteHistory.contains(hash);
  }

  public String getVoteHistory() {
    return getMetadata(META_VOTE_HISTORY, "");
  }

  public void setPublishTime(String publishTime) {
    putMetadata(META_PUBLISH_TIME, publishTime);
  }

  public void setPublishTime(Date publishTime) {
    putMetadata(META_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
  }

  public String getPublishTimeStr() {
    return getMetadata(META_PUBLISH_TIME);
  }

  public Instant getPublishTime() {
    String publishTimeStr = getPublishTimeStr();
    return DateTimeUtil.parseTime(publishTimeStr);
  }

  public String getReferrer() {
    return getMetadata(META_REFERRER);
  }

  public void setReferrer(String referrer) {
    putMetadata(META_REFERRER, referrer);
  }

  public int getFetchCount() {
    String referredPages = getMetadata(META_FETCH_TIMES);
    return StringUtil.tryParseInt(referredPages, 0);
  }

  public void setFetchCount(int count) {
    putMetadata(META_FETCH_TIMES, String.valueOf(count));
  }

  public void increaseFetchCount() {
    int count = getFetchCount();
    putMetadata(META_FETCH_TIMES, String.valueOf(count + 1));
  }

  public long getReferredArticles() {
    String referredPages = getMetadata(META_REFERRED_ARTICLES);
    return StringUtil.tryParseLong(referredPages, 0);
  }

  public void setReferredArticles(long count) {
    putMetadata(META_REFERRED_ARTICLES, String.valueOf(count));
  }

  public void increaseReferredArticles(long count) {
    long oldCount = getReferredArticles();
    putMetadata(META_REFERRED_ARTICLES, String.valueOf(oldCount + count));
  }

  public long getTotalOutLinkCount() {
    String outLinks = getMetadata(META_OUT_LINK_COUNT);
    return StringUtil.tryParseLong(outLinks, 0);
  }

  public void setTotalOutLinkCount(long count) {
    putMetadata(META_OUT_LINK_COUNT, String.valueOf(count));
  }

  public void increaseTotalOutLinkCount(long count) {
    long oldCount = getTotalOutLinkCount();
    putMetadata(META_OUT_LINK_COUNT, String.valueOf(oldCount + count));
  }

  public int sniffOutLinkCount() {
    int _a = page.getOutlinks().size();
    if (_a == 0) {
      Object count = page.getTemporaryVariable(VAR_OUTLINKS_COUNT);
      if (count != null && count instanceof Integer) {
        _a = (int)count;
      }
    }

    return _a;
  }

  public long getReferredChars() {
    String referredPages = getMetadata(META_REFERRED_CHARS);
    return StringUtil.tryParseLong(referredPages, 0);
  }

  public void setReferredChars(long count) {
    putMetadata(META_REFERRED_CHARS, String.valueOf(count));
  }

  public void increaseReferredChars(long count) {
    long oldCount = getReferredChars();
    setReferredChars(oldCount + count);
  }

  public Instant getReferredPublishTime() {
    String publishTimeStr = getMetadata(META_REFERRED_PUBLISH_TIME);
    return DateTimeUtil.parseTime(publishTimeStr);
  }

  public void setReferredPublishTime(Instant publishTime) {
    putMetadata(META_REFERRED_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
  }

  public boolean updateReferredPublishTime(Instant newPublishTime) {
    Instant latestTime = getReferredPublishTime();
    if (newPublishTime.isAfter(latestTime)) {
      setReferredPublishTime(newPublishTime);
      return true;
    }

    return false;
  }

  public Instant getFetchTime() {
    return Instant.ofEpochMilli(page.getFetchTime());
  }

  public Duration getFetchInterval() {
    return Duration.ofSeconds(page.getFetchInterval());
  }

  public Instant getHeaderLastModifiedTime(Instant defaultValue) {
    CharSequence lastModified = page.getHeaders().get(new Utf8(HttpHeaders.LAST_MODIFIED));
    if (lastModified != null) {
      return DateTimeUtil.parseTime(lastModified.toString());
    }

    return defaultValue;
  }

  public String getFetchTimeHistory(String defaultValue) {
    String s = getMetadata(Metadata.META_FETCH_TIME_HISTORY);
    return s == null ? defaultValue : s;
  }

  public void putFetchTimeHistory(Instant fetchTime) {
    String fetchTimeHistory = getMetadata(Metadata.META_FETCH_TIME_HISTORY);
    fetchTimeHistory = DateTimeUtil.constructTimeHistory(fetchTimeHistory, fetchTime, 10);
    putMetadata(Metadata.META_FETCH_TIME_HISTORY, fetchTimeHistory);
  }

  public Instant getFirstCrawlTime(Instant defaultValue) {
    Instant firstCrawlTime = null;

    String fetchTimeHistory = getFetchTimeHistory("");
    if (!fetchTimeHistory.isEmpty()) {
      String[] times = fetchTimeHistory.split(",");
      Instant time = DateTimeUtil.parseTime(times[0]);
      if (time.isAfter(Instant.EPOCH)) {
        firstCrawlTime = time;
      }
    }

    return firstCrawlTime == null ? defaultValue : firstCrawlTime;
  }

  public String getIndexTimeHistory(String defaultValue) {
    String s = getMetadata(Metadata.META_INDEX_TIME_HISTORY);
    return s == null ? defaultValue : s;
  }

  /**
   * TODO : consider hbase's record history feature
   * */
  public void putIndexTimeHistory(Instant indexTime) {
    String indexTimeHistory = getMetadata(Metadata.META_INDEX_TIME_HISTORY);
    indexTimeHistory = DateTimeUtil.constructTimeHistory(indexTimeHistory, indexTime, 10);
    putMetadata(Metadata.META_INDEX_TIME_HISTORY, indexTimeHistory);
  }

  public Instant getFirstIndexTime(Instant defaultValue) {
    Instant firstIndexTime = null;

    String indexTimeHistory = getIndexTimeHistory("");
    if (!indexTimeHistory.isEmpty()) {
      String[] times = indexTimeHistory.split(",");
      Instant time = DateTimeUtil.parseTime(times[0]);
      if (time.isAfter(Instant.EPOCH)) {
        firstIndexTime = time;
      }
    }

    return firstIndexTime == null ? defaultValue : firstIndexTime;
  }

  public void putMark(CharSequence key, CharSequence value) {
    page.getMarkers().put(key, value);
  }

  public CharSequence getMark(CharSequence key) {
    return page.getMarkers().get(key);
  }

  public String getHeader(String name, String defaultValue) {
    CharSequence value = page.getHeaders().get(new Utf8(name));
    return value == null ? defaultValue : value.toString();
  }

  public void putHeader(String name, String value) {
    page.getHeaders().put(new Utf8(name), new Utf8(value));
  }

  /**
   * TODO : Why the nutch team wrap the key using Utf8?
   * */
  public void putAllMetadata(Map<String, String> metadata) {
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String k = entry.getKey();
      String v = entry.getValue();

      putMetadata(k, v);
    }
  }

  public void putMetadata(String key, String value) {
    page.getMetadata().put(new Utf8(key), value == null ? null : ByteBuffer.wrap(value.getBytes()));
  }

  public void putMetadata(Utf8 key, ByteBuffer value) {
    page.getMetadata().put(key, value);
  }

  public String getMetadata(String key) {
    ByteBuffer bvalue = page.getMetadata().get(new Utf8(key));
    if (bvalue == null) {
      return null;
    }
    return Bytes.toString(bvalue.array());
  }

  public String getMetadata(String key, String defaultValue) {
    String value = getMetadata(key);
    return value == null ? defaultValue : value;
  }

  public boolean hasMetadata(String key) {
    ByteBuffer bvalue = page.getMetadata().get(new Utf8(key));
    return bvalue != null;
  }

  // But only delete when they exist. This is much faster for the underlying store
  // The markers are on the input anyway.
  public void clearMetadata(Utf8 key) {
    if (page.getMetadata().get(key) != null) {
      page.getMetadata().put(key, null);
    }
  }

  public void clearMetadata(String key) {
    if (getMetadata(key) != null) {
      putMetadata(key, null);
    }
  }

  // But only delete when they exist. This is much faster for the underlying store
  // The markers are on the input anyway.
  public void clearTmpMetadata() {
    page.getMetadata().keySet().stream()
        .filter(key -> key.toString().startsWith(Metadata.META_TMP))
        .forEach(key -> page.getMetadata().put(key, null));
  }

  public Map<String, String> getMetadata() {
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
