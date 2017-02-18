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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.persist.gora.GoraWebPage;
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

  public static Map<String, Object> convert(WebPage page, String... fields) {
    return convert(page, Lists.newArrayList(fields));
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

  public static Object convertNamedField(WebPage page, String field) {
    if (field.equals(GoraWebPage.Field.BASE_URL.getName())) {
      return page.getBaseUrl();
    } else if (field.equals(GoraWebPage.Field.PROTOCOL_STATUS.getName())) {
      return ProtocolStatusUtils.toString(page.getProtocolStatus());
    } else if (field.equals(GoraWebPage.Field.PARSE_STATUS.getName())) {
      return ParseStatusUtils.toString(page.getParseStatus());
    } else if (field.equals(GoraWebPage.Field.PREV_SIGNATURE.getName())) {
      return page.getPrevSignatureAsString();
    } else if (field.equals(GoraWebPage.Field.SIGNATURE.getName())) {
      return page.getSignatureAsString();
    } else if (field.equals(GoraWebPage.Field.PAGE_TITLE.getName())) {
      return page.getPageTitle();
    } else if (field.equals(GoraWebPage.Field.CONTENT_TITLE.getName())) {
      return page.getContentTitle();
    } else if (field.equals(GoraWebPage.Field.CONTENT_TEXT.getName())) {
      return page.getContentText();
    } else if (field.equals(GoraWebPage.Field.CONTENT.getName())) {
      return page.getContentAsString();
    } else if (field.equals(GoraWebPage.Field.PREV_FETCH_TIME.getName())) {
      return page.getPrevFetchTime();
    } else if (field.equals(GoraWebPage.Field.FETCH_TIME.getName())) {
      return page.getFetchTime();
    } else if (field.equals(GoraWebPage.Field.FETCH_INTERVAL.getName())) {
      return page.getFetchInterval();
    } else if (field.equals(GoraWebPage.Field.PREV_MODIFIED_TIME.getName())) {
      return page.getPrevModifiedTime();
    } else if (field.equals(GoraWebPage.Field.MODIFIED_TIME.getName())) {
      return page.getModifiedTime();
    } else if (field.equals(GoraWebPage.Field.CONTENT_PUBLISH_TIME.getName())) {
      return page.getContentPublishTime();
    } else if (field.equals(GoraWebPage.Field.PREV_CONTENT_PUBLISH_TIME.getName())) {
      return page.getPrevContentPublishTime();
    } else if (field.equals(GoraWebPage.Field.CONTENT_MODIFIED_TIME.getName())) {
      return page.getContentModifiedTime();
    } else if (field.equals(GoraWebPage.Field.PREV_CONTENT_MODIFIED_TIME.getName())) {
      return page.getPrevContentModifiedTime();
    } else if (field.equals(GoraWebPage.Field.CONTENT_TYPE.getName())) {
      return page.getContentType();
    } else if (field.equals(GoraWebPage.Field.PAGE_CATEGORY.getName())) {
      return page.getPageCategory();
    } else if (field.equals(GoraWebPage.Field.REFERRER.getName())) {
      return page.getReferrer();
    } else if (field.equals(GoraWebPage.Field.REFERRED_ARTICLES.getName())) {
      return page.getRefArticles();
    } else if (field.equals(GoraWebPage.Field.REFERRED_CHARS.getName())) {
      return page.getRefChars();
    } else if (field.equals(GoraWebPage.Field.REF_CONTENT_PUBLISH_TIME.getName())) {
      return page.getRefContentPublishTime();
    } else if (field.equals(GoraWebPage.Field.PREV_REF_CONTENT_PUBLISH_TIME.getName())) {
      return page.getPrevRefContentPublishTime();
    } else if (field.equals(GoraWebPage.Field.INLINKS.getName())) {
      return convertToStringsMap(page.getInlinks());
    } else if (field.equals(GoraWebPage.Field.OUTLINKS.getName())) {
      return convertToStringsMap(page.getOutlinks());
    } else if (field.equals(GoraWebPage.Field.HEADERS.getName())) {
      return convertToStringsMap(page.getHeaders());
    } else if (field.equals(GoraWebPage.Field.MARKERS.getName())) {
      return convertToStringsMap(page.getMarkers());
    } else if (field.equals(GoraWebPage.Field.METADATA.getName())) {
      return convertMetadata(page);
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

  public static Map<String, Object> convertMetadata(WebPage page) {
    Map<CharSequence, ByteBuffer> metadata = page.getMetadata();
    if (MapUtils.isEmpty(metadata)) {
      return Collections.emptyMap();
    }

    Map<String, Object> result = Maps.newHashMap();
    for (CharSequence key : metadata.keySet()) {
      Metadata.Name name = Metadata.Name.of(key.toString());
      String k = name.name().toLowerCase();
      Object v = name == Metadata.Name.UNKNOWN ? Bytes.toStringBinary(metadata.get(key)) : convertNamedMetadata(name, page);
      result.put(k, v);
    }

    return result;
  }

  public static Object convertNamedMetadata(Metadata.Name name, WebPage page) {
    switch (name) {
      case GENERATE_TIME:
        return page.getGenerateTime();
      case FETCH_TIME_HISTORY:
        return page.getFetchTimeHistory("");
      case TOTAL_OUT_LINKS:
        return page.getTotalOutLinks();
      case INDEX_TIME_HISTORY:
        return page.getIndexTimeHistory("");
      case CASH_KEY:
        return page.getCash();
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
      String value = entry.getValue() == null ? "null" : entry.getValue().toString();
      res.put(entry.getKey().toString(), value);
    }
    return res;
  }
}
