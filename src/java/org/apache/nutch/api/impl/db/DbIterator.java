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
package org.apache.nutch.api.impl.db;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.gora.query.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.model.request.DbFilter;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.RegexURLFilter;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.DbPageConverter;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.UnmodifiableIterator;

public class DbIterator extends UnmodifiableIterator<Map<String, Object>> {
  public static final Logger LOG = LoggerFactory.getLogger(DbIterator.class);

  public static long DefaultDbLimit = 100000L;

  private Result<String, WebPage> result;
  private WebPage page;

  private boolean hasNext;
  private long recordCount = 0;
  private long limit = DefaultDbLimit;
  private Utf8 batchId;
  private Set<String> fields;
  private RegexURLFilter urlFilter;

  DbIterator(Result<String, WebPage> res, DbFilter filter, Configuration conf) {
    this.result = res;
    if (filter.getFields() != null) { this.fields = filter.getFields(); }
    if (filter.getBatchId() != null) { this.batchId = new Utf8(filter.getBatchId()); }
    this.limit = filter.getLimit();

    String urlRegexRule = filter.getUrlFilter();
    if (urlRegexRule != null) {
      try {
        urlFilter = new RegexURLFilter(urlRegexRule);
      } catch (IllegalArgumentException | IOException e) {
        LOG.error(e.toString());
      }
    }

    try {
      skipNonRelevant();
    } catch (Exception e) {
      LOG.error("Cannot create db iterator!", e);
    }
  }

  private void skipNonRelevant() throws IOException, Exception {
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
    if (urlFilter != null && urlFilter.filter(url) == null) {
      return false;
    }

    Utf8 mark = Mark.UPDATEDB_MARK.checkMark(result.get());
    return batchId == null ? true : shouldProcess(mark, batchId);
  }

  public boolean hasNext() {
    return hasNext;
  }

  public boolean shouldProcess(CharSequence mark, Utf8 batchId) {
    if (mark == null) {
      return false;
    }

    boolean isAll = batchId.equals(Nutch.ALL_CRAWL_ID);
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

    page = WebPage.newBuilder(result.get()).build();

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
    Map<String, Object> result = DbPageConverter.convertPage(page, fields);

    if (CollectionUtils.isEmpty(fields) || fields.contains("url")) {
      result.put("url", TableUtil.unreverseUrl(url));
    }

    return result;
  }

  public static void main(String[] args) throws Exception {
    Pattern urlPattern = Pattern.compile("^http://www.hahaertong.com/goods/(\\d+)$");
    String url = "http://www.hahaertong.com/goods/6669";
    if (urlPattern.matcher(url).matches()) {
      System.out.println("good");
    }
    else {
      System.out.println("bad");
    }
  }
}
