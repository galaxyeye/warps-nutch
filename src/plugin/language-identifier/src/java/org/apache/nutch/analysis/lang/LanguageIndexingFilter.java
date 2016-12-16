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
package org.apache.nutch.analysis.lang;

// Nutch imports

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.gora.GoraWebPage;

import java.util.Collection;
import java.util.HashSet;

/**
 * An {@link IndexingFilter} that adds a
 * <code>lang</code> (language) field to the document.
 * 
 * It tries to find the language of the document by checking if
 * {@link HTMLLanguageParser} has added some language information
 * 
 * @author Sami Siren
 * @author Jerome Charron
 */
public class LanguageIndexingFilter implements IndexingFilter {

  private Configuration conf;

  private static final Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.METADATA);
  }

  /**
   * Constructs a new Language Indexing Filter.
   */
  public LanguageIndexingFilter() {
  }

  public IndexDocument filter(IndexDocument doc, String url, WebPage page)
      throws IndexingException {

    // check if LANGUAGE found, possibly put there by HTMLLanguageParser
//    ByteBuffer blang = page.get().getMetadata().get(new Utf8(Metadata.LANGUAGE));
//    String lang = Bytes.toString(blang.array());

    String lang = page.getMetadata(Metadata.LANGUAGE);

    if (lang.length() == 0) {
      lang = "unknown";
    }

    doc.add("lang", lang);

    return doc;
  }

  public Collection<GoraWebPage.Field> getFields() {
    return FIELDS;
  }

  public void addIndexBackendOptions(Configuration conf) {
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

}
