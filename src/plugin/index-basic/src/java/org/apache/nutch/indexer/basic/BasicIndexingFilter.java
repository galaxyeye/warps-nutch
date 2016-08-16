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

package org.apache.nutch.indexer.basic;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

// import org.apache.solr.common.util.DateUtil;

/**
 * Adds basic searchable fields to a document. The fields are: host - add host
 * as un-stored, indexed and tokenized url - url is both stored and indexed, so
 * it's both searchable and returned. This is also a required field. content -
 * content is indexed, so that it's searchable, but not stored in index title -
 * title is stored and indexed cache - add cached content/summary display
 * policy, if available tstamp - add timestamp when fetched, for deduplication
 */
public class BasicIndexingFilter implements IndexingFilter {

  public static final Logger LOG = LoggerFactory.getLogger(BasicIndexingFilter.class);

  private int MAX_TITLE_LENGTH;
  private int MAX_CONTENT_LENGTH;
  private Configuration conf;
  private boolean addDomain;

  private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(WebPage.Field.TITLE);
    FIELDS.add(WebPage.Field.TEXT);
    FIELDS.add(WebPage.Field.CONTENT);
    FIELDS.add(WebPage.Field.FETCH_TIME);
    FIELDS.add(WebPage.Field.CONTENT_TYPE);
  }

  /**
   * The {@link BasicIndexingFilter} filter object which supports boolean
   * configurable value for length of characters permitted within the title @see
   * {@code indexer.max.title.length} in nutch-default.xml
   * 
   * @param doc
   *          The {@link IndexDocument} object
   * @param url
   *          URL to be filtered for anchor text
   * @param page
   *          {@link WebPage} object relative to the URL
   * @return filtered NutchDocument
   */
  public IndexDocument filter(IndexDocument doc, String url, WebPage page) throws IndexingException {

    String reprUrl = TableUtil.toString(page.getReprUrl());
    String reprUrlString = reprUrl != null ? reprUrl.toString() : null;
    String urlString = url.toString();

    String host;
    try {
      URL u;
      if (reprUrlString != null) {
        u = new URL(reprUrlString);
      } else {
        u = new URL(urlString);
      }

      if (addDomain) {
        doc.add("domain", URLUtil.getDomainName(u));
      }

      host = u.getHost();
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }

    if (host != null) {
      doc.add("host", host);
    }

    // url is both stored and indexed, so it's both searchable and returned
    doc.add("url", reprUrlString == null ? urlString : reprUrlString);

    // content
    // String content = TableUtil.toString(page.getText());
    String content = Bytes.toString(page.getContent().array());
    try {
      // TODO : we should do this in nutch
      // Extract the article content using boilerpipe
      content = ArticleExtractor.INSTANCE.getText(content);
    } catch (BoilerpipeProcessingException e) {
      LOG.warn("Failed to extract text content by boilerpipe, " + e.getMessage());
    }

    if (content.length() == 0) {
      content = TableUtil.toString(page.getText());
    }

    if (MAX_CONTENT_LENGTH > -1 && content.length() > MAX_CONTENT_LENGTH) {
      content = content.substring(0, MAX_CONTENT_LENGTH);
    }
    doc.add("content", StringUtil.cleanField(content));

    // title
    String title = TableUtil.toString(page.getTitle());
    if (MAX_TITLE_LENGTH > -1 && title.length() > MAX_TITLE_LENGTH) {
      // truncate　title if needed
      title = title.substring(0, MAX_TITLE_LENGTH);
    }
    if (title.length() > 0) {
      // NUTCH-1004 Do not index empty values for title field
      doc.add("title", title);
    }

    // add cached content/summary display policy, if available
    ByteBuffer cachingRaw = page.getMetadata().get(Nutch.CACHING_FORBIDDEN_KEY_UTF8);
    if (cachingRaw != null) {
      String caching = Bytes.toString(cachingRaw.array());
      if (!caching.equals(Nutch.CACHING_FORBIDDEN_NONE)) {
        doc.add("cache", caching);
      }
    }

    // get content type
    doc.add("content_type", page.getContentType().toString());

    // add timestamp when fetched, for deduplication
    // String tstamp = DateUtil.getThreadLocalDateFormat().format(new Date(page.getFetchTime()));
    // add timestamp when fetched, for deduplication
    doc.add("tstamp", new Date(page.getFetchTime()));

    return doc;
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.MAX_TITLE_LENGTH = conf.getInt("indexer.max.title.length", 100);
    this.MAX_CONTENT_LENGTH = conf.getInt("indexer.max.content.length", 10 * 10000);
    this.addDomain = conf.getBoolean("indexer.add.domain", true);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "MAX_TITLE_LENGTH", MAX_TITLE_LENGTH,
        "MAX_CONTENT_LENGTH", MAX_CONTENT_LENGTH,
        "addDomain", addDomain
    ));
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Gets all the fields for a given {@link WebPage} Many datastores need to
   * setup the mapreduce job by specifying the fields needed. All extensions
   * that work on WebPage are able to specify what fields they need.
   */
  @Override
  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }

}
