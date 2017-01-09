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
package org.apache.nutch.persist;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.filter.CrawlFilter;
import org.apache.nutch.filter.PageCategory;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.persist.gora.ParseStatus;
import org.apache.nutch.persist.gora.ProtocolStatus;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.jetbrains.annotations.Contract;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nutch.metadata.Metadata.*;
import static org.apache.nutch.metadata.Metadata.Name.*;
import static org.apache.nutch.metadata.Nutch.DOC_FIELD_TEXT_CONTENT;
import static org.apache.nutch.metadata.Nutch.DOC_FIELD_TEXT_CONTENT_LENGTH;

/**
 * TODO : re-design the table schema to avoid hiding fields in metadata field and to improve efficiency
 * */
public class WebPage {

  private String url = "";
  private String reversedUrl = "";
  private GoraWebPage page;

  /**
   * Manual added to keep extracted document fields, this field is non-persistentable
   */
  private Map<String, Object> programVariables = new HashMap<>();

  /**
   * Initialize a WebPage with the underlying GoraWebPage instance.
   */
  public WebPage(GoraWebPage page) {
    this.page = page;
  }

  public WebPage(String url, GoraWebPage page, boolean urlReversed) {
    this.url = urlReversed ? TableUtil.unreverseUrl(url) : url;
    this.reversedUrl = urlReversed ? url : TableUtil.reverseUrlOrEmpty(url);
    this.page = page;
  }

  @Contract(" -> !null")
  public static WebPage newWebPage() {
    return new WebPage(GoraWebPage.newBuilder().build());
  }

  @Contract("_ -> !null")
  public static WebPage newWebPage(GoraWebPage page) {
    return new WebPage(GoraWebPage.newBuilder(page).build());
  }

  @Contract("_ -> !null")
  public static WebPage wrap(GoraWebPage page) {
    return new WebPage(page);
  }

  @Contract("_, _, _ -> !null")
  public static WebPage wrap(String url, GoraWebPage page, boolean urlReversed) {
    return new WebPage(url, page, urlReversed);
  }

  public String url() { return url; }

  public String reversedUrl() { return reversedUrl; }

  public GoraWebPage get() { return page; }

  public boolean isEmpty() {
    return page == null;
  }

  public Utf8 getBaseUrlUtf8() {
    return page.getBaseUrl() == null ? u8("") : (Utf8) page.getBaseUrl();
  }

  public String getBaseUrl() {
    // TODO : Check the necessary of TableUtil#toString
    return page.getBaseUrl() == null ? "" : TableUtil.toString(page.getBaseUrl());
  }

  public void setBaseUrl(Utf8 value) {
    page.setBaseUrl(value);
  }

  public void setBaseUrl(String value) { page.setBaseUrl(u8(value)); }

  public Integer getStatus() {
    return page.getStatus();
  }

  public void setStatus(Integer value) {
    page.setStatus(value);
  }

  public Instant getFetchTime() {
    return Instant.ofEpochMilli(page.getFetchTime());
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

  public Duration getFetchInterval() {
    return Duration.ofSeconds(page.getFetchInterval());
  }

  public long getFetchInterval(TimeUnit destUnit) {
    return destUnit.convert(page.getFetchInterval(), TimeUnit.SECONDS);
  }

  public void setFetchInterval(long interval) {
    page.setFetchInterval((int) interval);
  }

  public void setFetchInterval(float interval) {
    page.setFetchInterval(Math.round(interval));
  }

  public void setFetchInterval(Duration interval) {
    page.setFetchInterval((int) interval.getSeconds());
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
   */
  public ByteBuffer getContent() {
    return page.getContent();
  }

  public byte[] getContentAsBytes() {
    return Bytes.getBytes(page.getContent());
  }

  public String getContentAsString() {
    return Bytes.toString(getContentAsBytes());
  }

  public void setContent(ByteBuffer value) {
    page.setContent(value);
  }

  public void setContent(byte[] value) { page.setContent(ByteBuffer.wrap(value)); }

  public void setContent(String value) {
    setContent(value.getBytes());
  }

  public String getContentType() {
    return page.getContentType() == null ? "" : TableUtil.toString(page.getContentType());
  }

  public void setContentType(String value) {
    page.setContentType(u8(value.trim().toLowerCase()));
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
   */
  public ByteBuffer getSignature() {
    return page.getSignature();
  }

  public String getSignatureAsString() {
    return getSignature() == null ? "" : StringUtil.toHexString(getSignature());
  }

  public void setSignature(byte[] value) {
    page.setSignature(ByteBuffer.wrap(value));
  }

  public String getTitle() {
    return page.getTitle() == null ? "" : page.getTitle().toString();
  }

  public void setTitle(String value) {
    if (value != null) page.setTitle(u8(value));
  }

  public String getText() {
    return page.getText() == null ? "" : page.getText().toString();
  }

  public void setText(String value) {
    if (value != null) page.setText(u8(value));
  }

  public ParseStatus getParseStatus() {
    return page.getParseStatus();
  }

  public void setParseStatus(ParseStatus value) {
    page.setParseStatus(value);
  }

  public float getScore() { return page.getScore(); }

  public void setScore(float value) { page.setScore(value); }

  public float getArticleScore() { return getFloatMetadata(Name.ARTICLE_SCORE, 0.0f); }

  public void setArticleScore(float value) { setFloatMetadata(Name.ARTICLE_SCORE, value); }

  public CharSequence getReprUrl() {
    return page.getReprUrl();
  }

  public void setReprUrl(String value) {
    page.setReprUrl(u8(value));
  }

  public Map<CharSequence, ByteBuffer> getMetadata() {
    return page.getMetadata();
  }

  /**
   * Header information returned from the web server used to server the content which is subsequently fetched from.
   * This includes keys such as
   * TRANSFER_ENCODING,
   * CONTENT_ENCODING,
   * CONTENT_LANGUAGE,
   * CONTENT_LENGTH,
   * CONTENT_LOCATION,
   * CONTENT_DISPOSITION,
   * CONTENT_MD5,
   * CONTENT_TYPE,
   * LAST_MODIFIED
   * and LOCATION.
   */
  public Map<CharSequence, CharSequence> getHeaders() {
    return page.getHeaders();
  }

  /**
   * Header information returned from the web server used to server the content which is subsequently fetched from.
   * This includes keys such as
   * TRANSFER_ENCODING,
   * CONTENT_ENCODING,
   * CONTENT_LANGUAGE,
   * CONTENT_LENGTH,
   * CONTENT_LOCATION,
   * CONTENT_DISPOSITION,
   * CONTENT_MD5,
   * CONTENT_TYPE,
   * LAST_MODIFIED
   * and LOCATION.
   * * @param value the value to set.
   */
  public void setHeaders(Map<CharSequence, CharSequence> value) {
    page.setHeaders(value);
  }

  /**
   * Embedded hyperlinks which direct outside of the current domain.
   */
  public Map<CharSequence, CharSequence> getOutlinks() { return page.getOutlinks(); }

  /**
   * Embedded hyperlinks which direct outside of the current domain.   * @param value the value to set.
   */
  public void setOutlinks(Map<CharSequence, CharSequence> value) { page.setOutlinks(value); }

  public String getOldOutLinks() { return getMetadata(Name.FETCHED_OUT_LINKS, ""); }

  public void putOldOutLinks(Collection<CharSequence> outLinks) {
    putMetadata(Name.FETCHED_OUT_LINKS, getOldOutLinks() + "\n" + StringUtils.join(outLinks, "\n"));
  }

  public Map<CharSequence, CharSequence> getInlinks() { return page.getInlinks(); }

  public void setInlinks(Map<CharSequence, CharSequence> value) { page.setInlinks(value); }

  public Map<CharSequence, CharSequence> getMarkers() {
    return page.getMarkers();
  }

  public void setMarkers(Map<CharSequence, CharSequence> value) {
    page.setMarkers(value);
  }

  public String getBatchId() {
    return page.getBatchId() == null ? "" : page.getBatchId().toString();
  }

  /**
   * What's the difference between String and Utf8?
   */
  public void setBatchId(String value) {
    page.setBatchId(value);
  }

  public Map<String, Object> getProgramVariables() {
    return programVariables;
  }

  public Object getTempVar(String name) {
    return programVariables.get(name);
  }

  public <T> T getTempVar(String name, T defaultValue) {
    Object o = programVariables.get(name);
    return o == null ? defaultValue : (T) o;
  }

  public String getTempVarAsString(String name) {
    return getTempVarAsString(name, "");
  }

  public String getTempVarAsString(String name, String defaultValue) {
    Object value = programVariables.get(name);
    return value == null ? defaultValue : value.toString();
  }

  public void setTempVar(String name, Object value) {
    programVariables.put(name, value);
  }

  /*******************************************************************************************************/
  /*
  /*******************************************************************************************************/
  public void markAsSeed() {
    putMetadata(Name.IS_SEED, YES_STRING);
  }

  public boolean isSeed() {
    return hasMetadata(Name.IS_SEED);
  }

  public void setTextContentLength(int length) {
    putMetadata(Name.TEXT_CONTENT_LENGTH, String.valueOf(length));
  }

  public int getTextContentLength() {
    String ds = getMetadata(Name.TEXT_CONTENT_LENGTH);
    return StringUtil.tryParseInt(ds, 0);
  }

  public int sniffTextLength() {
    int length = getTextContentLength();
    if (length > 10) {
      return length;
    }

    Object obj = getTempVar(DOC_FIELD_TEXT_CONTENT_LENGTH);
    if (obj != null && obj instanceof Integer) {
      return ((Integer) obj);
    }

    obj = getTempVar(DOC_FIELD_TEXT_CONTENT);
    if (obj != null && obj instanceof String) {
      return ((String) obj).length();
    }

    CharSequence text = page.getText();
    if (text != null) {
      return text.length();
    }

    return 0;
  }

  public float getPageCategoryLikelihood() {
    return getFloatMetadata(Name.PAGE_CATEGORY_LIKELIHOOD, 0f);
  }

  public void setPageCategoryLikelihood(float likelihood) {
    setFloatMetadata(Name.PAGE_CATEGORY_LIKELIHOOD, likelihood);
  }

  public boolean isDetailPage(float threshold) {
    return getPageCategory().isDetail() && getPageCategoryLikelihood() >= threshold;
  }

  public boolean veryLikeDetailPage(String url) {
    return isDetailPage(0.85f) || CrawlFilter.sniffPageCategory(url, this).isDetail();
  }

  public void setPageCategory(PageCategory pageCategory) {
    putMetadata(PAGE_CATEGORY, pageCategory.name());
  }

  public PageCategory getPageCategory() {
    try {
      return PageCategory.valueOf(getMetadata(PAGE_CATEGORY));
    } catch (Throwable e) {
      return PageCategory.UNKNOWN;
    }
  }

  public void setNoMoreFetch() {
    putMetadata(FETCH_NO_MORE, YES_STRING);
  }

  public boolean noMoreFetch() {
    return hasMetadata(FETCH_NO_MORE);
  }

  public int getDepth() { return getDistance(); }

  public void setDepth(int newDepth) {
    setDistance(newDepth);
  }

  public int getDistance() {
    String ds = getMetadata(DISTANCE);
    return StringUtil.tryParseInt(ds, MAX_DISTANCE);
  }

  public void setDistance(int newDistance) {
    putMetadata(DISTANCE, String.valueOf(newDistance));
  }

  public void updateDistance(int newDistance) {
    int oldDistance = getDistance();
    if (newDistance < oldDistance) {
      setDistance(newDistance);
    }
  }

  public int sniffFetchPriority() {
    int priority = getFetchPriority(FETCH_PRIORITY_DEFAULT);

    int depth = getDepth();
    if (depth < FETCH_PRIORITY_DEPTH_BASE) {
      priority = Math.max(priority, FETCH_PRIORITY_DEPTH_BASE - depth);
    }

    return priority;
  }

  public void setFetchPriority(int priority) {
    putMetadata(Name.FETCH_PRIORITY, String.valueOf(priority));
  }

  public int getFetchPriority(int defaultPriority) {
    String s = getMetadata(Name.FETCH_PRIORITY);
    return StringUtil.tryParseInt(s, defaultPriority);
  }

  public void setGenerateTime(long generateTime) {
    putMetadata(Name.GENERATE_TIME, String.valueOf(generateTime));
  }

  public long getGenerateTime() {
    String generateTimeStr = getMetadata(Name.GENERATE_TIME);
    return StringUtil.tryParseLong(generateTimeStr, -1);
  }

  public float getCash() {
    return getFloatMetadata(Name.CASH_KEY, 0f);
  }

  public void setCash(float cash) {
    setFloatMetadata(Name.CASH_KEY, cash);
  }

  public Instant getPublishTime() { return DateTimeUtil.parseTime(getMetadata(Name.PUBLISH_TIME), Instant.EPOCH); }

  public void setPublishTime(Instant publishTime) {
    putMetadata(Name.PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
  }

  public void updatePublishTime(Instant newPublishTime) {
    Instant publishTime = getPublishTime();
    if (newPublishTime.isAfter(publishTime) && newPublishTime.isAfter(MIN_ARTICLE_PUBLISH_TIME)) {
      setPrevPublishTime(publishTime);
      setPublishTime(newPublishTime);
    }
  }

  public Instant getPrevPublishTime() {
    return DateTimeUtil.parseTime(getMetadata(Name.PREV_PUBLISH_TIME), Instant.EPOCH);
  }

  public void setPrevPublishTime(Instant publishTime) {
    putMetadata(Name.PREV_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
  }

  public String getReferrer() {
    return getMetadata(Name.REFERRER);
  }

  public void setReferrer(String referrer) {
    putMetadata(Name.REFERRER, referrer);
  }

  public int getFetchCount() {
    String referredPages = getMetadata(Name.FETCH_COUNT);
    return StringUtil.tryParseInt(referredPages, 0);
  }

  public void setFetchCount(int count) {
    putMetadata(Name.FETCH_COUNT, String.valueOf(count));
  }

  public void increaseFetchCount() {
    int count = getFetchCount();
    putMetadata(Name.FETCH_COUNT, String.valueOf(count + 1));
  }

  public long getRefArticles() {
    String refPages = getMetadata(Name.REFERRED_ARTICLES);
    return StringUtil.tryParseLong(refPages, 0);
  }

  public void setRefArticles(long count) {
    putMetadata(Name.REFERRED_ARTICLES, String.valueOf(count));
  }

  public void increaseRefArticles(long count) {
    long oldCount = getRefArticles();
    putMetadata(Name.REFERRED_ARTICLES, String.valueOf(oldCount + count));
  }

  public long getTotalOutLinkCount() {
    String outLinks = getMetadata(Name.OUT_LINK_COUNT);
    return StringUtil.tryParseLong(outLinks, 0);
  }

  public void setTotalOutLinkCount(long count) {
    putMetadata(Name.OUT_LINK_COUNT, String.valueOf(count));
  }

  public void increaseTotalOutLinkCount(long count) {
    long oldCount = getTotalOutLinkCount();
    setTotalOutLinkCount(oldCount + count);
  }

  public int sniffOutLinkCount() {
    int _a = page.getOutlinks().size();
    if (_a == 0) {
      Object count = getTempVar(VAR_OUTLINKS_COUNT);
      if (count != null && count instanceof Integer) {
        _a = (int) count;
      }
    }

    return _a;
  }

  public long getRefChars() {
    String referredPages = getMetadata(Name.REFERRED_CHARS);
    return StringUtil.tryParseLong(referredPages, 0);
  }

  public void setRefChars(long count) {
    putMetadata(Name.REFERRED_CHARS, String.valueOf(count));
  }

  public void increaseRefChars(long count) {
    long oldCount = getRefChars();
    setRefChars(oldCount + count);
  }

  public Instant getRefPublishTime() {
    String time = getMetadata(Name.REFERRED_PUBLISH_TIME);
    return DateTimeUtil.parseTime(time, Instant.EPOCH);
  }

  public void setRefPublishTime(Instant publishTime) {
    putMetadata(Name.REFERRED_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
  }

  public Instant getPrevRefPublishTime() {
    String time = getMetadata(Name.PREV_REFERRED_PUBLISH_TIME);
    return DateTimeUtil.parseTime(time, Instant.EPOCH);
  }

  public void setPrevRefPublishTime(Instant publishTime) {
    putMetadata(Name.PREV_REFERRED_PUBLISH_TIME, DateTimeUtil.solrCompatibleFormat(publishTime));
  }

  public boolean updateRefPublishTime(Instant newRefPublishTime) {
    Instant latestTime = getRefPublishTime();
    if (newRefPublishTime.isAfter(latestTime)) {
      setPrevRefPublishTime(latestTime);
      setRefPublishTime(newRefPublishTime);

      updatePublishTime(newRefPublishTime);

      return true;
    }

    return false;
  }

  public Instant getHeaderLastModifiedTime(Instant defaultValue) {
    CharSequence lastModified = page.getHeaders().get(u8(HttpHeaders.LAST_MODIFIED));
    if (lastModified != null) {
      return DateTimeUtil.parseTime(lastModified.toString(), Instant.EPOCH);
    }

    return defaultValue;
  }

  public String getFetchTimeHistory(String defaultValue) {
    String s = getMetadata(Name.FETCH_TIME_HISTORY);
    return s == null ? defaultValue : s;
  }

  public void putFetchTimeHistory(Instant fetchTime) {
    String fetchTimeHistory = getMetadata(Name.FETCH_TIME_HISTORY);
    fetchTimeHistory = DateTimeUtil.constructTimeHistory(fetchTimeHistory, fetchTime, 10);
    putMetadata(Name.FETCH_TIME_HISTORY, fetchTimeHistory);
  }

  public Instant getFirstCrawlTime(Instant defaultValue) {
    Instant firstCrawlTime = null;

    String fetchTimeHistory = getFetchTimeHistory("");
    if (!fetchTimeHistory.isEmpty()) {
      String[] times = fetchTimeHistory.split(",");
      Instant time = DateTimeUtil.parseTime(times[0], Instant.EPOCH);
      if (time.isAfter(Instant.EPOCH)) {
        firstCrawlTime = time;
      }
    }

    return firstCrawlTime == null ? defaultValue : firstCrawlTime;
  }

  public String getIndexTimeHistory(String defaultValue) {
    String s = getMetadata(Name.INDEX_TIME_HISTORY);
    return s == null ? defaultValue : s;
  }

  /**
   * TODO : consider hbase's record history feature
   */
  public void putIndexTimeHistory(Instant indexTime) {
    String indexTimeHistory = getMetadata(Name.INDEX_TIME_HISTORY);
    indexTimeHistory = DateTimeUtil.constructTimeHistory(indexTimeHistory, indexTime, 10);
    putMetadata(Name.INDEX_TIME_HISTORY, indexTimeHistory);
  }

  public Instant getFirstIndexTime(Instant defaultValue) {
    Instant firstIndexTime = null;

    String indexTimeHistory = getIndexTimeHistory("");
    if (!indexTimeHistory.isEmpty()) {
      String[] times = indexTimeHistory.split(",");
      Instant time = DateTimeUtil.parseTime(times[0], Instant.EPOCH);
      if (time.isAfter(Instant.EPOCH)) {
        firstIndexTime = time;
      }
    }

    return firstIndexTime == null ? defaultValue : firstIndexTime;
  }

  public Utf8 getMark(Mark mark) { return (Utf8) page.getMarkers().get(wrapKey(mark)); }

  public boolean hasMark(Mark mark) {
    return getMark(mark) != null;
  }

  public void putMark(Mark mark, String value) {
    putMark(mark, wrapValue(value));
  }

  public void putMark(Mark mark, Utf8 value) {
    page.getMarkers().put(wrapKey(mark), value);
  }

  public void putMarkIfNonNull(Mark mark, Utf8 value) {
    if (value != null) {
      putMark(mark, value);
    }
  }

  public void removeMark(Mark mark) {
    if (hasMark(mark)) {
      page.getMarkers().put(wrapKey(mark), null);
    }
  }

  public String getHeader(String name, String defaultValue) {
    CharSequence value = page.getHeaders().get(u8(name));
    return value == null ? defaultValue : value.toString();
  }

  public void putHeader(String name, String value) {
    page.getHeaders().put(u8(name), u8(value));
  }

  /**
   * TODO : Why the nutch team use key as Utf8?
   */
  public void putAllMetadata(Map<String, String> metadata) {
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String k = entry.getKey();
      String v = entry.getValue();

      putMetadata(k, v);
    }
  }

  public void putMetadata(Name name, String value) {
    putMetadata(name.value(), value);
  }

  public void putMetadata(String key, String value) {
    page.getMetadata().put(u8(key), value == null ? null : ByteBuffer.wrap(value.getBytes()));
  }

  public void putMetadata(Utf8 key, ByteBuffer value) {
    page.getMetadata().put(key, value);
  }

  public ByteBuffer getRawMetadata(Name name) {
    return getRawMetadata(name.value());
  }

  public ByteBuffer getRawMetadata(String name) {
    return page.getMetadata().get(u8(name));
  }

  public String getMetadata(Name name) {
    return getMetadata(name.value());
  }

  public String getMetadata(String name) {
    ByteBuffer bvalue = getRawMetadata(name);
    return bvalue == null ? null : Bytes.toString(bvalue.array());
  }

  public String getMetadata(Name name, String defaultValue) {
    String value = getMetadata(name);
    return value == null ? defaultValue : value;
  }

  public boolean hasMetadata(Name name) {
    return hasMetadata(name.value());
  }

  public boolean hasMetadata(String key) {
    return getRawMetadata(key) != null;
  }

  public void clearMetadata(CharSequence key) {
    if (page.getMetadata().get(key) != null) {
      page.getMetadata().put(key, null);
    }
  }

  public void clearMetadata(String key) {
    clearMetadata(u8(key));
  }

  public void clearMetadata(Name name) {
    clearMetadata(name.value());
  }

  // But only delete when they exist. This is much faster for the underlying store
  // The markers are on the input anyway.
  public void clearTmpMetadata() {
    getMetadata().keySet().stream()
        .filter(key -> key.toString().startsWith(Metadata.META_TMP))
        .forEach(this::clearMetadata);
  }

  public Map<String, String> getMetadataAsString() {
    return page.getMetadata().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().toString(), e -> Bytes.toString(e.getValue().array())));
  }

  public Map<String, String> getMetadataAsStringBinary() {
    return page.getMetadata().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().toString(), e -> Bytes.toStringBinary(e.getValue())));
  }

  public float getFloatMetadata(Name name, float defaultValue) {
    return getFloatMetadata(name.value(), defaultValue);
  }

  public float getFloatMetadata(String name, float defaultValue) {
    ByteBuffer raw = page.getMetadata().get(u8(name));
    float value = defaultValue;
    if (raw != null) {
      value = Bytes.toFloat(raw.array(), raw.arrayOffset() + raw.position());
    }
    return value;
  }

  public void setFloatMetadata(Name name, float value) {
    setFloatMetadata(name.value(), value);
  }

  public void setFloatMetadata(String key, float value) {
    page.getMetadata().put(u8(key), ByteBuffer.wrap(Bytes.toBytes(value)));
  }

  @Contract("_ -> !null")
  public static Utf8 wrapKey(String key) {
    return u8(key);
  }

  public static Utf8 wrapKey(Name name) {
    return u8(name.value());
  }

  public static Utf8 wrapKey(Mark mark) {
    return u8(mark.value());
  }

  @Contract("_ -> !null")
  public static Utf8 wrapValue(String value) {
    return u8(value);
  }

  /** What's the difference between String and Utf8? */
  private static Utf8 u8(String value) {
    if (value == null) {
      return null;
    }
    return new Utf8(value);
  }

  public static Map<String, String> convertToStringsMap(Map<?, ?> map) {
    return map.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
  }

  @Override
  public String toString() { return page.toString(); }
}
