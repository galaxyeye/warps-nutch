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

package org.apache.nutch.indexer.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.*;

import static org.apache.nutch.metadata.Nutch.PARAM_NUTCH_JOB_NAME;

/**
 * Indexer which can be configured to extract metadata from the crawldb, parse
 * metadata or content metadata. You can specify the properties "index.db",
 * "index.parse" or "index.content" who's values are comma-delimited
 * <value>key1,key2,key3</value>.
 */
public class MetadataIndexer implements IndexingFilter {
  private static final String PARSE_CONF_PROPERTY = "index.metadata";
  private static final String INDEX_PREFIX = "meta_";
  private static final String PARSE_META_PREFIX = "meta_";

  private static SiteNames siteNames;
  private static ResourceCategory resourceCategory;
  private static Map<String, String> parseFieldnames = new TreeMap<>();

  private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(WebPage.Field.METADATA);
    FIELDS.add(WebPage.Field.FETCH_TIME);
    FIELDS.add(WebPage.Field.CONTENT_TYPE);
  }

  private Configuration conf;
  private NutchMetrics nutchMetrics;
  private String reportSuffix;

  public void setConf(Configuration conf) {
    this.conf = conf;

    conf.getStringCollection(PARSE_CONF_PROPERTY).forEach(metatag -> {
      String key = PARSE_META_PREFIX + metatag.toLowerCase(Locale.ROOT);
      String value = INDEX_PREFIX + metatag;

      parseFieldnames.put(key, value);
    });

    siteNames = new SiteNames(conf);
    resourceCategory = new ResourceCategory(conf);

    this.nutchMetrics = NutchMetrics.getInstance(conf);
    this.reportSuffix = conf.get(PARAM_NUTCH_JOB_NAME, "job-unknown-" + DateTimeUtil.now("MMdd.HHmm"));

//    LOG.info(StringUtil.formatAsLine(
//        "className", this.getClass().getSimpleName(),
//        "siteNames", siteNames.count(),
//        "resourceCategory", resourceCategory.count()
//    ));
  }

  public IndexDocument filter(IndexDocument doc, String url, WebPage page) throws IndexingException {
    try {
      addTime(doc, url, page);

      addHost(doc, url, page);

      // Metadata-indexer does not meet all our requirement
      addGeneralMetadata(doc, url, page);

      addPageMetadata(doc, url, page);
    }
    catch (IndexingException e) {
      LOG.error(e.toString());
    }

    return doc;
  }

  private void addHost(IndexDocument doc, String url, WebPage page) throws IndexingException {
    String reprUrlString = page.getReprUrl() != null ? page.getReprUrl().toString() : null;

    url = reprUrlString == null ? url : reprUrlString;

    if (url == null || url.isEmpty()) {
      return;
    }

    try {
      URL u = new URL(url);

      String domain = URLUtil.getDomainName(u);

      doc.add("domain", domain);
      doc.add("url", url);
      doc.addIfNotNull("site_name", siteNames.getSiteName(domain));
      doc.addIfNotNull("resource_category", resourceCategory.getCategory(url));
      doc.addIfNotNull("host", u.getHost());
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }
  }

  private void addTime(IndexDocument doc, String url, WebPage page) {
    Instant now = Instant.now();

    String crawlTimeStr = DateTimeUtil.solrCompatibleFormat(now);
    Instant firstCrawlTime = TableUtil.getFirstCrawlTime(page, now);
    String fetchTimeHistory = TableUtil.getFetchTimeHistory(page, crawlTimeStr);

    doc.add("first_crawl_time", DateTimeUtil.solrCompatibleFormat(firstCrawlTime));
    doc.add("last_crawl_time", crawlTimeStr);
    doc.add("fetch_time_history", fetchTimeHistory);

    String indexTimeStr = DateTimeUtil.solrCompatibleFormat(now);
    Instant firstIndexTime = TableUtil.getFirstIndexTime(page, now);
    String indexTimeHistory = TableUtil.getIndexTimeHistory(page, indexTimeStr);

    doc.add("first_index_time", DateTimeUtil.solrCompatibleFormat(firstIndexTime));
    doc.add("last_index_time", indexTimeStr);
    doc.add("index_time_history", indexTimeHistory);
  }

  private void addGeneralMetadata(IndexDocument doc, String url, WebPage page) throws IndexingException {
    String contentType = TableUtil.toString(page.getContentType());
    if (contentType == null || !contentType.contains("html")) {
      LOG.warn("Content type " + contentType + " is not fully supported");
      // return doc;
    }

    // doc.add("encoding", page.getTemporaryVariableAsString("encoding", ""));

    // get content type
    doc.add("content_type", contentType);
  }

  private IndexDocument addPageMetadata(IndexDocument doc, String url, WebPage page) {
    if (doc == null || parseFieldnames.isEmpty()) {
      return doc;
    }

    for (Map.Entry<String, String> metatag : parseFieldnames.entrySet()) {
      String k = metatag.getValue();
      String metadata = TableUtil.getMetadata(page, metatag.getKey());

      if (k != null && metadata != null) {
        k = k.trim();
        metadata = metadata.trim();

        if (!k.isEmpty() && !metadata.isEmpty()) {
          final String finalK = k;

          // TODO : avoid this dirty hard coding
          if (finalK.equalsIgnoreCase("meta_description")) {
            Arrays.stream(metadata.split("\t")).forEach(v -> doc.addIfAbsent(finalK, v));
          }
          else {
            Arrays.stream(metadata.split("\t")).forEach(v -> doc.add(finalK, v));
          }
        }
      } // if
    } // for

    return doc;
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }
}
