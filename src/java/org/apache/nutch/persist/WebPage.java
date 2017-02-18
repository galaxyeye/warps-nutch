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
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.filter.PageCategory;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.persist.gora.ParseStatus;
import org.apache.nutch.persist.gora.ProtocolStatus;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nutch.metadata.Metadata.*;

/**
 * TODO : re-design the table schema to avoid hiding fields in metadata field and to improve efficiency
 * */
public class WebPage {

  private static ZoneId zoneId = ZoneId.systemDefault();
  private String url = "";
  private String reversedUrl = "";
  private GoraWebPage page;

  /**
   * Manual added to keep extracted document fields, this field is non-persistentable
   */
  private Map<String, Object> tempVars = new HashMap<>();

  /**
   * Initialize a WebPage with the underlying GoraWebPage instance.
   */
  public WebPage(GoraWebPage page) {
    this.page = page;
  }

  public WebPage(String url, GoraWebPage page, boolean urlReversed) {
    this.url = urlReversed ? TableUtil.unreverseUrl(url) : url;
    this.reversedUrl = urlReversed ? url : TableUtil.reverseUrlOrEmpty(url);
    assert (!reversedUrl.startsWith("http"));
    this.page = page;
  }

  public WebPage(String url, String reversedUrl, GoraWebPage page) {
    this.url = url;
    assert (!reversedUrl.startsWith("http"));
    this.reversedUrl = reversedUrl;
    this.page = page;
  }

  public static WebPage newWebPage() {
    return new WebPage(GoraWebPage.newBuilder().build());
  }

  public static WebPage newWebPage(String url) {
    return new WebPage(url, GoraWebPage.newBuilder().build(), false);
  }

  public static WebPage newWebPage(String url, float score) {
    WebPage page = new WebPage(url, GoraWebPage.newBuilder().build(), false);
    page.setScore(score);
    return page;
  }

  public static WebPage wrap(String url, String reversedUrl, GoraWebPage page) {
    return new WebPage(url, reversedUrl, page);
  }

  public static WebPage wrap(String url, GoraWebPage page, boolean urlReversed) {
    return new WebPage(url, page, urlReversed);
  }

  public String url() {
    return url != null ? url : "";
  }

  public String reversedUrl() {
    return reversedUrl != null ? reversedUrl : "";
  }

  public boolean hasLegalUrl() {
    return url != null && !url.isEmpty() && reversedUrl != null && !reversedUrl.isEmpty();
  }

  public GoraWebPage get() {
    return page;
  }

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

  public void setBaseUrl(String value) {
    page.setBaseUrl(u8(value));
  }

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

  public void setContent(byte[] value) {
    page.setContent(ByteBuffer.wrap(value));
  }

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

  public String getPrevSignatureAsString() {
    ByteBuffer sig = getPrevSignature();
    if (sig == null) {
      sig = ByteBuffer.wrap("".getBytes());
    }
    return StringUtil.toHexString(sig);
  }

  public void setPrevSignature(ByteBuffer value) {
    page.setPrevSignature(value);
  }

  /**
   * An implementation of a WebPage's signature from which it can be identified and referenced at any point in time.
   * This is essentially the WebPage's fingerprint represnting its state for any point in time.
   */
  public ByteBuffer getSignature() { return page.getSignature(); }

  public String getSignatureAsString() {
    ByteBuffer sig = getSignature();
    if (sig == null) {
      sig = ByteBuffer.wrap("".getBytes());
    }
    return StringUtil.toHexString(sig);
  }

  public void setSignature(byte[] value) {
    page.setSignature(ByteBuffer.wrap(value));
  }

  public String getPageTitle() {
    return page.getPageTitle() == null ? "" : page.getPageTitle().toString();
  }

  public void setPageTitle(String pageTitle) {
    if (pageTitle != null) page.setPageTitle(u8(pageTitle));
  }

  public String getContentTitle() {
    return page.getContentTitle() == null ? "" : page.getContentTitle().toString();
  }

  public void setContentTitle(String contentTitle) {
    if (contentTitle != null) {
      page.setContentTitle(u8(contentTitle));
    }
  }

  public String getPageText() {
    return page.getPageText() == null ? "" : page.getPageText().toString();
  }

  public void setPageText(String value) {
    if (value != null && !value.isEmpty()) page.setPageText(u8(value));
  }

  public void setContentText(String textContent) {
    if (textContent != null && !textContent.isEmpty()) page.setContentText(u8(textContent));
  }

  public String getContentText() { return page.getContentText() == null ? "" : page.getContentText().toString(); }

  public void setContentTextLength(int length) {
    page.setContentTextLength(length);
  }

  public int getContentTextLength() {
    return page.getContentTextLength();
  }

  public ParseStatus getParseStatus() { return page.getParseStatus(); }

  public void setParseStatus(ParseStatus value) {
    page.setParseStatus(value);
  }

  public float getScore() {
    return page.getScore();
  }

  public void setScore(float value) {
    page.setScore(value);
  }

  public float getContentScore() {
    return page.getContentScore() == null ? 0.0f : page.getContentScore();
  }

  public void setContentScore(float score) {
    page.setContentScore(score);
  }

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
  public Map<CharSequence, CharSequence> getOutlinks() {
    return page.getOutlinks();
  }

  /**
   * Embedded hyperlinks which direct outside of the current domain.   * @param value the value to set.
   */
  public void setOutlinks(Map<CharSequence, CharSequence> value) {
    page.setOutlinks(value);
  }

  public String getAllOutLinks() {
    return page.getOldOutlinks() == null ? "" : page.getOldOutlinks().toString();
  }

  public void setAllOutLinks(String value) {
    page.setOldOutlinks(value);
  }

  /**
   * Put all extracted, filtered links
   */
  public void addToAllOutLinks(Collection<CharSequence> outLinks) {
    String allLinks = getAllOutLinks() + "\n" + StringUtils.join(outLinks, "\n");
    // assume the avarage lenght of a link is 100 characters
    // If the old links string is too long, truncate it
    if (allLinks.length() > (10 * 1000) * 100) {
      int pos = StringUtils.indexOf(allLinks, '\n', allLinks.length() / 3);
      allLinks = allLinks.substring(pos + 1);
    }

    setAllOutLinks(allLinks);
  }

  public Map<CharSequence, CharSequence> getInlinks() {
    return page.getInlinks();
  }

  public void setInlinks(Map<CharSequence, CharSequence> value) {
    page.setInlinks(value);
  }

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

  public Map<String, Object> getTempVars() {
    return tempVars;
  }

  public Object getTempVar(String name) {
    return tempVars.get(name);
  }

  public <T> T getTempVar(String name, T defaultValue) {
    Object o = tempVars.get(name);
    return o == null ? defaultValue : (T) o;
  }

  public String getTempVarAsString(String name) {
    return getTempVarAsString(name, "");
  }

  public String getTempVarAsString(String name, String defaultValue) {
    Object value = tempVars.get(name);
    return value == null ? defaultValue : value.toString();
  }

  public void setTempVar(String name, Object value) {
    tempVars.put(name, value);
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

  public void setPageCategory(PageCategory pageCategory) {
    page.setPageCategory(pageCategory.name());
  }

  public PageCategory getPageCategory() {
    try {
      if (page.getPageCategory() != null) {
        return PageCategory.valueOf(page.getPageCategory().toString());
      }
    } catch (Throwable ignored) {
    }

    return PageCategory.UNKNOWN;
  }

  public int getDepth() {
    return getDistance();
  }

  public void setDepth(int newDepth) {
    setDistance(newDepth);
  }

  public int getDistance() {
    return page.getDistance() < 0 ? Nutch.DISTANCE_INFINITE : page.getDistance();
  }

  public void setDistance(int newDistance) {
    page.setDistance(newDistance);
  }

  public void updateDistance(int newDistance) {
    int oldDistance = getDistance();
    if (newDistance < oldDistance) {
      setDistance(newDistance);
    }
  }

  public void setFetchPriority(int priority) {
    page.setFetchPriority(priority);
  }

  public int getFetchPriority() {
    return page.getFetchPriority() > 0 ? page.getFetchPriority() : FETCH_PRIORITY_DEFAULT;
  }

  public int sniffFetchPriority() {
    int priority = getFetchPriority();

    int depth = getDepth();
    if (depth < FETCH_PRIORITY_DEPTH_BASE) {
      priority = Math.max(priority, FETCH_PRIORITY_DEPTH_BASE - depth);
    }

    return priority;
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

  public Instant getContentPublishTime() {
    return Instant.ofEpochMilli(page.getContentPublishTime());
  }

  public void setContentPublishTime(Instant publishTime) {
    page.setContentPublishTime(publishTime.toEpochMilli());
  }

  public void updateContentPublishTime(Instant newPublishTime) {
    Instant publishTime = getContentPublishTime();
    if (newPublishTime.isAfter(publishTime) && newPublishTime.isAfter(MIN_ARTICLE_PUBLISH_TIME)) {
      setPrevContentPublishTime(publishTime);
      setContentPublishTime(newPublishTime);
    }
  }

  public boolean updateRefContentPublishTime(Instant newRefPublishTime) {
    Instant latestRefPublishTime = getRefContentPublishTime();
    if (newRefPublishTime.isAfter(latestRefPublishTime)) {
      setPrevRefContentPublishTime(latestRefPublishTime);
      setRefContentPublishTime(newRefPublishTime);

      return true;
    }

    return false;
  }

  public Instant getPrevContentPublishTime() {
    return Instant.ofEpochMilli(page.getPrevContentPublishTime());
  }

  public void setPrevContentPublishTime(Instant publishTime) {
    page.setContentPublishTime(publishTime.toEpochMilli());
  }

  public Instant getRefContentPublishTime() {
    return Instant.ofEpochMilli(page.getRefContentPublishTime());
  }

  public void setRefContentPublishTime(Instant publishTime) {
    page.setRefContentPublishTime(publishTime.toEpochMilli());
  }

  public Instant getPrevRefContentPublishTime() {
    return Instant.ofEpochMilli(page.getPrevRefContentPublishTime());
  }

  public void setPrevRefContentPublishTime(Instant publishTime) {
    page.setPrevRefContentPublishTime(publishTime.toEpochMilli());
  }

  public Instant getContentModifiedTime() {
    return Instant.ofEpochMilli(page.getContentModifiedTime());
  }

  public void setContentModifiedTime(Instant modifiedTime) {
    page.setContentPublishTime(modifiedTime.toEpochMilli());
  }

  public Instant getPrevContentModifiedTime() {
    return Instant.ofEpochMilli(page.getPrevContentModifiedTime());
  }

  public void setPrevContentModifiedTime(Instant modifiedTime) {
    page.setPrevContentModifiedTime(modifiedTime.toEpochMilli());
  }

  public void updateContentModifiedTime(Instant newModifiedTime) {
    if (newModifiedTime.isAfter(getContentModifiedTime()) && newModifiedTime.isAfter(MIN_ARTICLE_PUBLISH_TIME)) {
      setContentModifiedTime(newModifiedTime);
    }
  }

  public String getReferrer() {
    return page.getReferrer() == null ? "" : page.getReferrer().toString();
  }

  public void setReferrer(String referrer) {
    if (referrer != null && referrer.length() > Nutch.SHORTEST_VALID_URL_LENGTH) {
      page.setReferrer(referrer);
    }
  }

  public int getFetchCount() {
    return page.getFetchCount();
  }

  public void setFetchCount(int count) {
    page.setFetchCount(count);
  }

  public void increaseFetchCount() {
    int count = getFetchCount();
    setFetchCount(count + 1);
  }

  public int getRefArticles() {
    return page.getReferredArticles();
  }

  public void setRefArticles(int count) {
    page.setReferredArticles(count);
  }

  public void increaseRefArticles(int count) {
    int oldCount = getRefArticles();
    setRefArticles(oldCount + count);
  }

  public long getTotalOutLinks() {
    String outLinks = getMetadata(Name.TOTAL_OUT_LINKS);
    return StringUtil.tryParseLong(outLinks, 0);
  }

  public void setTotalOutLinkCount(long count) {
    putMetadata(Name.TOTAL_OUT_LINKS, String.valueOf(count));
  }

  public void increaseTotalOutLinkCount(long count) {
    long oldCount = getTotalOutLinks();
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

  public int getRefChars() {
    return page.getReferredChars();
  }

  public void setRefChars(int count) {
    page.setReferredChars(count);
  }

  public void increaseRefChars(int count) {
    int oldCount = getRefChars();
    setRefChars(oldCount + count);
  }

  public Instant getHeaderLastModifiedTime(Instant defaultValue) {
    CharSequence lastModified = page.getHeaders().get(u8(HttpHeaders.LAST_MODIFIED));
    if (lastModified != null) {
      return DateTimeUtil.parseHttpDateTime(lastModified.toString(), Instant.EPOCH);
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
      Instant time = DateTimeUtil.parseInstant(times[0], Instant.EPOCH);
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
      Instant time = DateTimeUtil.parseInstant(times[0], Instant.EPOCH);
      if (time.isAfter(Instant.EPOCH)) {
        firstIndexTime = time;
      }
    }

    return firstIndexTime == null ? defaultValue : firstIndexTime;
  }

  public Utf8 getMark(Mark mark) {
    return (Utf8) page.getMarkers().get(wrapKey(mark));
  }

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
    putMetadata(name.text(), value);
  }

  public void putMetadata(String key, String value) {
    page.getMetadata().put(u8(key), value == null ? null : ByteBuffer.wrap(value.getBytes()));
  }

  public void putMetadata(Utf8 key, ByteBuffer value) {
    page.getMetadata().put(key, value);
  }

  public ByteBuffer getRawMetadata(Name name) {
    return getRawMetadata(name.text());
  }

  public ByteBuffer getRawMetadata(String name) {
    return page.getMetadata().get(u8(name));
  }

  public String getMetadata(Name name) {
    return getMetadata(name.text());
  }

  public String getMetadata(String name) {
    ByteBuffer bvalue = getRawMetadata(name);
    return bvalue == null ? null : Bytes.toString(bvalue.array());
  }

  public String getMetadata(Name name, String defaultValue) {
    String value = getMetadata(name);
    return value == null ? defaultValue : value;
  }

  public String getMetadata(String name, String defaultValue) {
    String value = getMetadata(name);
    return value == null ? defaultValue : value;
  }

  public boolean hasMetadata(Name name) {
    return hasMetadata(name.text());
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
    clearMetadata(name.text());
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
    return getFloatMetadata(name.text(), defaultValue);
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
    setFloatMetadata(name.text(), value);
  }

  public void setFloatMetadata(String key, float value) {
    page.getMetadata().put(u8(key), ByteBuffer.wrap(Bytes.toBytes(value)));
  }

  public static Utf8 wrapKey(String key) {
    return u8(key);
  }

  public static Utf8 wrapKey(Name name) {
    return u8(name.text());
  }

  public static Utf8 wrapKey(Mark mark) {
    return u8(mark.value());
  }

  public static Utf8 wrapValue(String value) {
    return u8(value);
  }

  /**
   * What's the difference between String and Utf8?
   */
  private static Utf8 u8(String value) {
    if (value == null) {
      return null;
    }
    return new Utf8(value);
  }

  public static Map<String, String> convertToStringsMap(Map<?, ?> map) {
    return map.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
  }

  public String getRepresentation() {
    return getRepresentation(true, true, true, true);
  }

  public String getRepresentation(boolean dumpText, boolean dumpContent, boolean dumpHeaders, boolean dumpLinks) {
    StringBuilder sb = new StringBuilder();
    sb.append("key:\t" + reversedUrl() + "\n");
    sb.append("baseUrl:\t" + getBaseUrl() + "\n");
    sb.append("status:\t" + getStatus() + " (" + CrawlStatus.getName(getStatus().byteValue()) + ")\n");
    sb.append("depth:\t" + getDepth() + "\n");
    sb.append("pageCategory:\t" + getPageCategory() + "\n");
    sb.append("fetchCount:\t" + getFetchCount() + "\n");
    sb.append("fetchPriority:\t" + getFetchPriority() + "\n");
    sb.append("fetchInterval:\t" + getFetchInterval() + "\n");
    sb.append("retriesSinceFetch:\t" + getRetriesSinceFetch() + "\n");

    sb.append("prevFetchTime:\t" + formatAsLocalDateTime(getPrevFetchTime()) + "\n");
    sb.append("fetchTime:\t" + formatAsLocalDateTime(getFetchTime()) + "\n");
    sb.append("prevModifiedTime:\t" + formatAsLocalDateTime(getPrevModifiedTime()) + "\n");
    sb.append("modifiedTime:\t" + formatAsLocalDateTime(getModifiedTime()) + "\n");
    sb.append("prevContentModifiedTime:\t" + formatAsLocalDateTime(getPrevContentModifiedTime(), ChronoUnit.MILLIS) + "\n");
    sb.append("contentModifiedTime:\t" + formatAsLocalDateTime(getContentModifiedTime(), ChronoUnit.MILLIS) + "\n");
    sb.append("prevContentPublishTime:\t" + formatAsLocalDateTime(getPrevContentPublishTime(), ChronoUnit.MILLIS) + "\n");
    sb.append("contentPublishTime:\t" + formatAsLocalDateTime(getContentPublishTime(), ChronoUnit.MILLIS) + "\n");

    sb.append("pageTitle:\t" + getPageTitle() + "\n");
    sb.append("contentTitle:\t" + getContentTitle() + "\n");

    sb.append("protocolStatus:\t" + ProtocolStatusUtils.toString(getProtocolStatus()) + "\n");
    sb.append("prevSignature:\t" + getPrevSignatureAsString() + "\n");
    sb.append("signature:\t" + getSignatureAsString() + "\n");
    sb.append("parseStatus:\t" + ParseStatusUtils.toString(getParseStatus()) + "\n");
    sb.append("score:\t" + getScore() + "\n");
    sb.append("contentScore:\t" + getContentScore() + "\n");

    Map<CharSequence, CharSequence> markers = getMarkers();
    if (markers != null) {
      for (Map.Entry<CharSequence, CharSequence> entry : markers.entrySet()) {
        sb.append("marker " + entry.getKey().toString() + " : \t" + entry.getValue() + "\n");
      }
    }
    sb.append("reprUrl:\t" + getReprUrl() + "\n");
    CharSequence batchId = getBatchId();
    if (batchId != null) {
      sb.append("batchId:\t" + batchId.toString() + "\n");
    }

    getMetadata().entrySet().forEach(e -> sb.append("metadata " + e.getKey() + ":\t" + Bytes.toString(e.getValue().array()) + "\n"));

    if (dumpLinks) {
      getOutlinks().entrySet().forEach(e -> sb.append("outlink:\t" + e.getKey() + "\t" + e.getValue() + "\n"));
      sb.append("Total " + getOutlinks().size() + " outlinks\n");
      getInlinks().entrySet().forEach(e -> sb.append("inlink:\t" + e.getKey() + "\t" + e.getValue() + "\n"));
      sb.append("Total " + getInlinks().size() + " inlinks\n");
      Stream.of(getAllOutLinks().split("\n")).filter(StringUtils::isNotBlank).forEach(l -> sb.append("allOutlink:\t" + l + "\n"));
      sb.append("Total " + StringUtils.countMatches(getAllOutLinks(), "\n") + " (all) outlinks\n");
    }

    if (dumpHeaders) {
      Map<CharSequence, CharSequence> headers = getHeaders();
      if (headers != null) {
        for (Map.Entry<CharSequence, CharSequence> e : headers.entrySet()) {
          sb.append("header:\t" + e.getKey() + "\t" + e.getValue() + "\n");
        }
      }
    }
    ByteBuffer content = getContent();
    if (content != null && dumpContent) {
      sb.append("contentType:\t" + getContentType() + "\n");
      sb.append("content:start:\n");
      sb.append(Bytes.toString(content.array()));
      sb.append("\ncontent:end:\n");
    }

    if (dumpText) {
      sb.append("contentText:start:\n");
      sb.append(getContentText());
      sb.append("\ncontentText:end:\n");

      sb.append("pageText:start:\n");
      sb.append(getPageText());
      sb.append("\npageText:end:\n");
    }

    return sb.toString();
  }
  
  private String formatAsLocalDateTime(Instant instant, TemporalUnit temporalUnit) {
    return LocalDateTime.ofInstant(instant.truncatedTo(temporalUnit), zoneId).toString();
  }
  
  private String formatAsLocalDateTime(Instant instant) {
    return formatAsLocalDateTime(instant, ChronoUnit.SECONDS);
  }

  @Override
  public String toString() {
    return getRepresentation(false, false, false, false);
  }
}
