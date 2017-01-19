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
package org.apache.nutch.persist.gora.db;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.persist.gora.WebPageConverter;
import org.apache.nutch.common.Params;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.apache.nutch.metadata.Nutch.PARAM_CRAWL_ID;

public class Db {

  public static final Logger LOG = LoggerFactory.getLogger(Db.class);

  private Configuration conf;
  private DataStore<String, GoraWebPage> store;

  public Db(Configuration conf, String crawlId) {
    this.conf = conf;
    if (crawlId != null && !crawlId.isEmpty()) {
      conf.set(PARAM_CRAWL_ID, crawlId);
    }

    try {
      store = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot create web store!", e);
    }
  }

  public WebPage get(String url) {
    String reversedUrl = TableUtil.reverseUrlOrEmpty(url);
    if (reversedUrl.isEmpty()) {
      LOG.warn("Failed to reverse url " + url);
      return WebPage.newWebPage();
    }

    return WebPage.wrap(store.get(reversedUrl));
  }

  public Map<String, Object> get(String url, Set<String> fields) {
    WebPage page = WebPage.wrap(store.get(TableUtil.reverseUrlOrEmpty(url)));

    Params.of(
        "crawlId", conf.get(PARAM_CRAWL_ID),
        "url", url,
        "fields", StringUtils.join(fields, ",")
    ).withLogger(LOG).info(true);

    return WebPageConverter.convert(page, fields);
  }

  public boolean update(String url, WebPage page) {
    String reversedUrl = TableUtil.reverseUrlOrEmpty(url);
    if (!reversedUrl.isEmpty() && !page.isEmpty()) {
      store.put(reversedUrl, page.get());
      return true;
    }

    return false;
  }

  public boolean delete(String url) {
    String reversedUrl = TableUtil.reverseUrlOrEmpty(url);
    if (!reversedUrl.isEmpty()) {
      store.delete(reversedUrl);
      return true;
    }

    return false;
  }

  public boolean truncate() {
    String schemaName = store.getSchemaName();
    if (schemaName.startsWith("tmp_") || schemaName.endsWith("_tmp_webpage")) {
      store.truncateSchema();
      LOG.info("Schema " + schemaName + " is truncated");
      return true;
    }
    else {
      LOG.info("Only schema name starts with tmp_ or ends with _tmp can be truncated using this API");
      return false;
    }
  }

  public DbIterator query(DbQuery filter) {
    Query<String, GoraWebPage> goraQuery = store.newQuery();

    String crawlId = conf.get(PARAM_CRAWL_ID);

    String startKey = TableUtil.reverseUrlOrEmpty(filter.getStartUrl());
    String endKey = TableUtil.reverseUrlOrEmpty(filter.getEndUrl());

    if (!startKey.isEmpty()) {
      goraQuery.setStartKey(startKey);
      if (!endKey.isEmpty()) {
        endKey = endKey.replaceAll("\\uFFFF", "\uFFFF");
        endKey = endKey.replaceAll("\\\\uFFFF", "\uFFFF");
        goraQuery.setEndKey(endKey);
      }
    }

    String[] fields = prepareFields(filter.getFields());
    goraQuery.setFields(fields);

    String urlFilter = filter.getUrlFilter();
    if (StringUtils.isEmpty(urlFilter)) {
      urlFilter = "+.";
    }
    conf.set("urlfilter.regex.rules", urlFilter);

    Params.of(
        "crawlId", crawlId,
        "startKey", startKey,
        "endKey", endKey,
        "fields", StringUtils.join(fields, ","),
        "urlFilter", urlFilter
    ).withLogger(LOG).info(true);

    Result<String, GoraWebPage> result = store.execute(goraQuery);
    return new DbIterator(result, filter, conf);
  }

  private String[] prepareFields(Set<String> fields) {
    if (CollectionUtils.isEmpty(fields)) {
      return GoraWebPage._ALL_FIELDS;
    }
    fields.remove("url");
    return fields.toArray(new String[fields.size()]);
  }
}
