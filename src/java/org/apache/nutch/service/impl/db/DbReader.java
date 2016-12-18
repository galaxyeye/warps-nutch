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
package org.apache.nutch.service.impl.db;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.service.model.request.DbFilter;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Set;

public class DbReader {

  public static final Logger LOG = LoggerFactory.getLogger(DbReader.class);

  private DataStore<String, GoraWebPage> store;

  public DbReader(Configuration conf, String crawlId) {
    conf = new Configuration(conf);
    if (crawlId != null) {
      LOG.debug("Crawl Id : " + crawlId);
      conf.set(Nutch.PARAM_CRAWL_ID, crawlId);
    }

    try {
      store = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot create webstore!", e);
    }
  }

  public DbIterator runQuery(DbFilter filter) {
    String startKey = filter.getStartKey();
    String endKey = filter.getEndKey();

    if (endKey != null) {
      // LOG.debug(endKey);
      endKey = endKey.replaceAll("\\uFFFF", "\uFFFF");
      endKey = endKey.replaceAll("\\\\uFFFF", "\uFFFF");
      // LOG.debug(endKey);
    }

    if (!filter.isKeysReversed()) {
      startKey = reverseKey(filter.getStartKey());
      endKey = reverseKey(filter.getEndKey());
    }

    Query<String, GoraWebPage> query = store.newQuery();

    query.setFields(prepareFields(filter.getFields()));
    if (startKey != null) {
      query.setStartKey(startKey);
      if (endKey != null) {
        endKey = endKey.replaceAll("\\uFFFF", "\uFFFF");
        endKey = endKey.replaceAll("\\\\uFFFF", "\uFFFF");
        query.setEndKey(endKey);
      }
    }

    LOG.info("Start key : " + startKey + ", end key : " + endKey);

    String urlFilter = filter.getUrlFilter();
    if (StringUtils.isEmpty(urlFilter)) {
      urlFilter = "+.";
    }

    Configuration conf = ConfigUtils.create();
    conf.set("urlfilter.regex.rules", urlFilter);

    Result<String, GoraWebPage> result = store.execute(query);
    return new DbIterator(result, filter, conf);
  }

  private String reverseKey(String key) {
    if (StringUtils.isEmpty(key)) {
      return null;
    }

    try {
      return TableUtil.reverseUrl(key);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Wrong url format!", e);
    }
  }

  private String[] prepareFields(Set<String> fields) {
    if (CollectionUtils.isEmpty(fields)) {
      return null;
    }
    fields.remove("url");
    return fields.toArray(new String[fields.size()]);
  }
}
