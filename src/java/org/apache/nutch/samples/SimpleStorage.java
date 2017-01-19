/**
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
 */

package org.apache.nutch.samples;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.common.Params;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.WebPageConverter;
import org.apache.nutch.persist.gora.db.Db;
import org.apache.nutch.persist.gora.db.DbIterator;
import org.apache.nutch.persist.gora.db.DbQuery;
import org.apache.nutch.persist.gora.db.DbQueryResult;
import org.apache.nutch.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nutch.metadata.Nutch.ALL_BATCH_ID_STR;
import static org.apache.nutch.metadata.Nutch.PARAM_CRAWL_ID;

/**
 * Parser checker, useful for testing parser. It also accurately reports
 * possible fetching and parsing failures and presents protocol status signals
 * to aid debugging. The tool enables us to retrieve the following data from any
 */
public class SimpleStorage {

  public static final Logger LOG = LoggerFactory.getLogger(SimpleStorage.class);

  private String crawlId;
  private Db db;

  public SimpleStorage(String crawlId, Configuration conf) {
    this.crawlId = StringUtils.isEmpty(crawlId) ? conf.get(PARAM_CRAWL_ID) : crawlId;
    this.db = new Db(conf, crawlId);
    Params.of("crawlId", crawlId).withLogger(LOG).info(true);
  }

  public SimpleStorage(Configuration conf) {
    this(conf.get(PARAM_CRAWL_ID), conf);
  }

  public WebPage get(String url) {
    Params.of("url", url).withLogger(LOG).info(true);

    return db.get(url);
  }

  public boolean remove(String url) {
    return db.delete(url);
  }

  public boolean truncate() {
    return db.truncate();
  }

  public DbQueryResult scan(String startUrl, String endUrl) {
    DbQueryResult result = new DbQueryResult();
    DbQuery filter = new DbQuery(crawlId, ALL_BATCH_ID_STR, startUrl, endUrl);

    Params.of("crawlId", crawlId, "startUrl", startUrl, "endUrl", endUrl).withLogger(LOG).info(true);

    DbIterator iterator = db.query(filter);

    long ignoreCount = 0L;
    while (++ignoreCount < filter.getStart() && iterator.hasNext()) {
      iterator.skipNext();
    }

    while (iterator.hasNext()) {
      result.addValue(iterator.next());
    }

    return result;
  }

  public DbQueryResult query(DbQuery query) {
    DbQueryResult result = new DbQueryResult();
    DbIterator iterator = db.query(query);

    long ignoreCount = 0L;
    while (++ignoreCount < query.getStart() && iterator.hasNext()) {
      iterator.skipNext();
    }

    while (iterator.hasNext()) {
      result.addValue(iterator.next());
    }

    return result;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage : Db [-crawlId <crawlId>] <-get|-remove|-scan|-truncate|-query> [url]");
      System.exit(0);
    }

    String crawlId = null;
    String command = "";
    String url = "";

    for (int i = 0; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      }
      else {
        command = args[i];
        if (command.equals("-get") || command.equals("-remove")) {
          url = args[++i];
        }
      }
    }

    SimpleStorage storage = new SimpleStorage(crawlId, ConfigUtils.create());

    switch (command) {
      case "-get":
        WebPage page = storage.get(url);
        if (!page.isEmpty()) {
          System.out.println(WebPageConverter.convert(page));
        }
        break;
      case "-remove":
        storage.remove(url);
        break;
      case "-scan":
//      DbQueryResult result = storage.scan(url);
//      System.out.println(result.getValues());
        System.out.println("Not supported yet");
        break;
      case "-truncate":
        storage.truncate();
        break;
      case "-query":
        System.out.println("Not supported yet");
        break;
    }
  }
}
