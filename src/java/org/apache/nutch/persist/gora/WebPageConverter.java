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
package org.apache.nutch.persist.gora;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.protocol.ProtocolStatusUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

public class WebPageConverter {

  public static Set<String> ALL_FIELDS = new HashSet<>();

  static {
    Collections.addAll(ALL_FIELDS, GoraWebPage._ALL_FIELDS);
    ALL_FIELDS.remove(GoraWebPage.Field.CONTENT.getName());
  }

  public static Map<String, Object> convert(WebPage page) {
    return convert(page, ALL_FIELDS);
  }

  public static Map<String, Object> convert(WebPage page, Collection<String> fields) {
    Map<String, Object> result = Maps.newHashMap();
    for (Schema.Field field : filterFields(page, fields)) {
      Object value = convertField(page, field);
      if (value != null) {
        result.put(field.name(), value);
      }
    }
    return result;
  }

  private static Object convertField(WebPage page, Schema.Field field) {
    int index = field.pos();
    if (index < 0) {
      return null;
    }

    Object value = page.get().get(index);
    if (value == null) {
      return null;
    }

    Object o = convertNamedField(page, field.name());
    if (o != null) {
      return o;
    }

    if (value instanceof Utf8) {
      return value.toString();
    }

    if (value instanceof ByteBuffer) {
      return Bytes.toStringBinary((ByteBuffer) value);
    }

    return value;
  }

  private static Object convertNamedField(WebPage page, String field) {
    if (field.equals(GoraWebPage.Field.BASE_URL.getName())) {
      return page.getBaseUrl();
    }
    else if (field.equals(GoraWebPage.Field.METADATA.getName())) {
      return getMetadata(page);
    }
    else if (field.equals(GoraWebPage.Field.PROTOCOL_STATUS.getName())) {
      return ProtocolStatusUtils.toString(page.getProtocolStatus());
    }
    else if (field.equals(GoraWebPage.Field.PARSE_STATUS.getName())) {
      return ParseStatusUtils.toString(page.getParseStatus());
    }
    else if (field.equals(GoraWebPage.Field.PREV_SIGNATURE.getName())) {
      return page.setPrevSignatureAsString();
    }
    else if (field.equals(GoraWebPage.Field.SIGNATURE.getName())) {
      return page.getSignatureAsString();
    }
    else if (field.equals(GoraWebPage.Field.TITLE.getName())) {
      return page.getTitle();
    }
    else if (field.equals(GoraWebPage.Field.TEXT.getName())) {
      return page.getText();
    }
    else if (field.equals(GoraWebPage.Field.CONTENT.getName())) {
      return page.getContentAsString();
    }
    else if (field.equals(GoraWebPage.Field.MARKERS.getName()))  {
      return convertToStringsMap(page.getMarkers());
    }
    else if (field.equals(GoraWebPage.Field.PREV_FETCH_TIME.getName())) {
      return page.getPrevFetchTime();
    }
    else if (field.equals(GoraWebPage.Field.FETCH_TIME.getName())) {
      return page.getFetchTime();
    }
    else if (field.equals(GoraWebPage.Field.FETCH_INTERVAL.getName())) {
      return page.getFetchInterval();
    }
    else if (field.equals(GoraWebPage.Field.PREV_MODIFIED_TIME.getName())) {
      return page.getPrevModifiedTime();
    }
    else if (field.equals(GoraWebPage.Field.MODIFIED_TIME.getName())) {
      return page.getModifiedTime();
    }
    else if (field.equals(GoraWebPage.Field.INLINKS.getName())) {
      return convertToStringsMap(page.getInlinks());
    }
    else if (field.equals(GoraWebPage.Field.OUTLINKS.getName())) {
      return convertToStringsMap(page.getOutlinks());
    }
    else if (field.equals(GoraWebPage.Field.HEADERS.getName())) {
      return convertToStringsMap(page.getHeaders());
    }

    return null;
  }

  private static Set<Schema.Field> filterFields(WebPage page, Collection<String> queryFields) {
    List<Schema.Field> pageFields = page.get().getSchema().getFields();
    if (CollectionUtils.isEmpty(queryFields)) {
      return Sets.newHashSet(pageFields);
    }

    Set<Schema.Field> filteredFields = Sets.newLinkedHashSet();
    for (Schema.Field field : pageFields) {
      if (queryFields.contains(field.name())) {
        filteredFields.add(field);
      }
    }
    return filteredFields;
  }

  private static Map<String, Object> getMetadata(WebPage page) {
    Map<CharSequence, ByteBuffer> metadata = page.getMetadata();
    if (MapUtils.isEmpty(metadata)) {
      return Collections.emptyMap();
    }

    Map<String, Object> result = Maps.newHashMap();
    for (CharSequence key : metadata.keySet()) {
      String k = key.toString();
      Metadata.Name name = Metadata.Name.of(key.toString());
      Object v = name == Metadata.Name.UNKNOWN ? Bytes.toStringBinary(metadata.get(key)) : getNamedMetadata(name, page);
      result.put(k, v);
    }

    return result;
  }

  private static Object getNamedMetadata(Metadata.Name name, WebPage page) {
    switch (name) {
      case IS_SEED:
        return page.isSeed();
      case IS_DETAIL:
        return "UNEXPECTED METADATA NAME IS_DETAIL";
      case GENERATE_TIME:
        return page.getGenerateTime();
      case DISTANCE:
        return page.getDistance();
      case FETCH_TIME_HISTORY:
        return page.getFetchTimeHistory("");
      case FETCH_COUNT:
        return page.getFetchCount();
      case FETCH_PRIORITY:
        return page.getFetchPriority(0);
      case TOTAL_OUT_LINKS:
        return page.getTotalOutLinks();
      case OLD_OUT_LINKS:
        return page.getOldOutLinks();
      case INDEX_TIME_HISTORY:
        return page.getIndexTimeHistory("");
      case ARTICLE_SCORE:
        return page.getArticleScore();
      case CASH_KEY:
        return page.getCash();
      case PAGE_CATEGORY:
        return page.getPageCategory();
      case PAGE_CATEGORY_LIKELIHOOD:
        return page.getPageCategoryLikelihood();
      case TEXT_CONTENT_LENGTH:
        return page.getTextContentLength();
      case REFERRER:
        return page.getReferrer();
      case REFERRED_ARTICLES:
        return page.getRefArticles();
      case REFERRED_CHARS:
        return page.getRefChars();
      case REFERRED_PUBLISH_TIME:
        return page.getRefPublishTime();
      case PREV_REFERRED_PUBLISH_TIME:
        return page.getPrevRefPublishTime();
      case PUBLISH_TIME:
        return page.getPublishTime();
      case PREV_PUBLISH_TIME:
        return page.getPrevRefPublishTime();
      case META_KEYWORDS:
        return page.getMetadata(Metadata.Name.META_KEYWORDS);
      case META_DESCRIPTION:
        return page.getMetadata(Metadata.Name.META_DESCRIPTION);
      case UNKNOWN:
        return "";
      default:
        return page.getMetadata(name);
    }
  }

  private static Map<String, String> convertToStringsMap(Map<?, ?> map) {
    Map<String, String> res = Maps.newHashMap();
    for (Entry<?, ?> entry : map.entrySet()) {
      res.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return res;
  }
}
