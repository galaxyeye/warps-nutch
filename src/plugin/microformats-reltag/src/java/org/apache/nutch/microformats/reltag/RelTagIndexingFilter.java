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
package org.apache.nutch.microformats.reltag;

// Nutch imports

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.storage.WrappedWebPage;
import org.apache.nutch.storage.gora.GoraWebPage;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;

/**
 * An {@link IndexingFilter} that adds <code>tag</code>
 * field(s) to the document.
 * 
 * @see <a href="http://www.microformats.org/wiki/rel-tag">
 *      http://www.microformats.org/wiki/rel-tag</a>
 * @author J&eacute;r&ocirc;me Charron
 */
public class RelTagIndexingFilter implements IndexingFilter {

  private Configuration conf;

  private static final Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.BASE_URL);
    FIELDS.add(GoraWebPage.Field.METADATA);
  }

  /**
   * Gets all the fields for a given {@link WrappedWebPage} Many datastores need to
   * setup the mapreduce job by specifying the fields needed. All extensions
   * that work on WrappedWebPage are able to specify what fields they need.
   */
  @Override
  public Collection<GoraWebPage.Field> getFields() {
    return FIELDS;
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * The {@link RelTagIndexingFilter} filter object.
   * 
   * @param doc
   *          The {@link IndexDocument} object
   * @param url
   *          URL to be filtered for rel-tag's
   * @param page
   *          {@link WrappedWebPage} object relative to the URL
   * @return filtered NutchDocument
   */
  @Override
  public IndexDocument filter(IndexDocument doc, String url, WrappedWebPage page)
      throws IndexingException {
    // Check if some Rel-Tags found, possibly put there by RelTagParser
    ByteBuffer bb = page.get().getMetadata().get(new Utf8(RelTagParser.REL_TAG));

    if (bb != null) {
      String[] tags = Bytes.toString(bb.array()).split("\t");
      for (int i = 0; i < tags.length; i++) {
        doc.add("tag", tags[i]);
      }
    }
    return doc;
  }
}
