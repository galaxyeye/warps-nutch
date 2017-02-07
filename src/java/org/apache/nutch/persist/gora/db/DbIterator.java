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

import com.google.common.collect.UnmodifiableIterator;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.gora.query.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.filter.URLFilterException;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.persist.gora.WebPageConverter;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.apache.nutch.metadata.Nutch.ALL_BATCH_ID;

public class DbIterator extends UnmodifiableIterator<Map<String, Object>> {

  public static final Logger LOG = LoggerFactory.getLogger(Db.class);

  public static long DefaultDbLimit = 1000L;

  private Result<String, GoraWebPage> result;
  private WebPage page;

  private boolean hasNext;
  private long recordCount = 0;
  private long limit = DefaultDbLimit;
  private Utf8 batchId;
  private Set<String> fields;

  DbIterator(Result<String, GoraWebPage> res, DbQuery filter, Configuration conf) {
    this.result = res;
    if (filter.getFields() != null) { this.fields = filter.getFields(); }
    if (filter.getBatchId() != null) { this.batchId = new Utf8(filter.getBatchId()); }
    this.limit = filter.getLimit();

    // LOG.info("regex : " + urlRegexRule + ", batchId : " + batchId);

    try {
      skipNonRelevant();
    } catch (Exception e) {
      LOG.error("Cannot create read iterator!", e);
    }
  }

  private void skipNonRelevant() throws Exception {
    do {
      hasNext = result.next();
      String skey = result.getKey();
      if (skey == null) {
        break;
      }

      String url = TableUtil.unreverseUrl(result.getKey());
      if (isRelevant(url)) {
        break;
      }
    }
    while (hasNext);
  }

  private boolean isRelevant(String url) throws URLFilterException {
//    if (urlFilter != null && urlFilter.filter(url) == null) {
//      ++urlRegexNotMatch;
//      return false;
//    }

    WebPage page = WebPage.wrap(result.get());
    Utf8 mark = page.getMark(Mark.UPDATEOUTG);
    return batchId == null || shouldProcess(mark, batchId);
  }

  public boolean hasNext() {
    return hasNext;
  }

  public boolean shouldProcess(Utf8 mark, Utf8 batchId) {
    if (mark == null) {
      return false;
    }

    boolean isAll = batchId.equals(ALL_BATCH_ID);
    if (!isAll && !mark.equals(batchId)) {
      return false;
    }

    return true;
  }

  public void skipNext() {
    String skey = result.getKey();
    if (skey == null) {
      return;
    }

    try {
      skipNonRelevant();

      if (!hasNext) {
        result.close();
      }
    } catch (Exception e) {
      LOG.error("Cannot get next result!", e);
      hasNext = false;
    }
  }

  public Map<String, Object> next() {
    String skey = result.getKey();
    if (skey == null) {
      return null;
    }

    page = WebPage.wrap(GoraWebPage.newBuilder(result.get()).build());

    try {
      skipNonRelevant();

      if (!hasNext || recordCount >= limit) {
        result.close();
      }
    } catch (Exception e) {
      LOG.error("Cannot get next result!", e);
      hasNext = false;
      return null;
    }

    ++recordCount;
    return pageAsMap(skey, page);
  }

  private Map<String, Object> pageAsMap(String url, WebPage page) {
    Map<String, Object> result = WebPageConverter.convert(page, fields);

    if (CollectionUtils.isEmpty(fields) || fields.contains("url")) {
      result.put("url", TableUtil.unreverseUrl(url));
    }

    return result;
  }
}