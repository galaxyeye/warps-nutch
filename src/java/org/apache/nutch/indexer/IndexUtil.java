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
package org.apache.nutch.indexer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;

/**
 * Utility to create an indexed document from a webpage.
 * 
 */
public class IndexUtil {
  private static final Log LOG = LogFactory.getLog(IndexUtil.class);

  private IndexingFilters filters;
  private ScoringFilters scoringFilters;

  public IndexUtil(Configuration conf) {
    filters = new IndexingFilters(conf);
    scoringFilters = new ScoringFilters(conf);
  }

  /**
   * Index a {@link WebPage}, here we add the following fields:
   * <ol>
   * <li><tt>id</tt>: default uniqueKey for the {@link NutchDocument}.</li>
   * <li><tt>digest</tt>: Digest is used to identify pages (like unique ID) and
   * is used to remove duplicates during the dedup procedure. It is calculated
   * using {@link org.apache.nutch.crawl.MD5Signature} or
   * {@link org.apache.nutch.crawl.TextProfileSignature}.</li>
   * <li><tt>batchId</tt>: The page belongs to a unique batchId, this is its
   * identifier.</li>
   * <li><tt>boost</tt>: Boost is used to calculate document (field) score which
   * can be used within queries submitted to the underlying indexing library to
   * find the best results. It's part of the scoring algorithms. See
   * scoring.link, scoring.opic, scoring.tld, etc.</li>
   * </ol>
   * 
   * @param key
   *          The key of the page (reversed url).
   * @param page
   *          The {@link WebPage}.
   * @return The indexed document, or null if skipped by index filters.
   */
  public NutchDocument index(String key, WebPage page) {
    NutchDocument doc = new NutchDocument(key);
    doc.add("id", key);
    doc.add("digest", StringUtil.toHexString(page.getSignature()));

    if (page.getBatchId() != null) {
      doc.add("batchId", page.getBatchId().toString());
    }

    String url = doc.getUrl();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Indexing URL: " + url);
    }

    float boost = 1.0f;

    try {
      doc = filters.filter(doc, url, page);
      boost = scoringFilters.indexerScore(url, doc, page, boost);

      doc.setScore(boost);
      // store boost for use by explain and dedup
      doc.add("boost", Float.toString(boost));
    } catch (IndexingException | ScoringFilterException e) {
      LOG.warn("Failed to indexing " + key + ": " + StringUtil.stringifyException(e));
    }

    return doc;
  }
}
