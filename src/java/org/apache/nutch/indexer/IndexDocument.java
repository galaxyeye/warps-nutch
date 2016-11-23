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

import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/** A {@link IndexDocument} is the unit of indexing. */
public class IndexDocument implements Writable, Iterable<Entry<String, IndexField>> {

  public static final byte VERSION = 2;

  private String key = "";

  private String url = "";

  private Map<String, IndexField> fields = new LinkedMap<>();

  private Metadata documentMeta = new Metadata();

  private float weight = 1.0f;

  public IndexDocument() {
  }

  public IndexDocument(String key) {
    this.key = key;
    this.url = TableUtil.unreverseUrl(key);
  }

  public String getKey() {
    return key;
  }

  public String getUrl() {
    return url;
  }

  public void addIfAbsent(String name, Object value) {
    IndexField field = fields.get(name);
    if (field == null) {
      field = new IndexField(value);
      fields.put(name, field);
    }
  }

  public void addIfNotEmpty(String name, String value) {
    if (value == null || value.isEmpty()) {
      return;
    }

    IndexField field = fields.get(name);
    if (field == null) {
      field = new IndexField(value);
      fields.put(name, field);
    } else {
      field.add(value);
    }
  }

  public void addIfNotNull(String name, Object value) {
    if (value == null) {
      return;
    }
    IndexField field = fields.get(name);
    if (field == null) {
      field = new IndexField(value);
      fields.put(name, field);
    } else {
      field.add(value);
    }
  }

  public void add(String name, Object value) {
    IndexField field = fields.get(name);
    if (field == null) {
      field = new IndexField(value);
      fields.put(name, field);
    } else {
      field.add(value);
    }
  }

  public Object getFieldValue(String name) {
    IndexField field = fields.get(name);
    if (field == null) {
      return null;
    }
    if (field.getValues().size() == 0) {
      return null;
    }
    return field.getValues().get(0);
  }

  public IndexField getField(String name) {
    return fields.get(name);
  }

  public IndexField removeField(String name) {
    return fields.remove(name);
  }

  public Collection<String> getFieldNames() {
    return fields.keySet();
  }

  public List<Object> getFieldValues(String name) {
    IndexField field = fields.get(name);
    if (field == null) {
      return null;
    }

    return field.getValues();
  }

  public String getFieldValueAsString(String name) {
    IndexField field = fields.get(name);
    if (field == null || field.getValues().isEmpty()) {
      return null;
    }

    return field.getValues().iterator().next().toString();
  }

  /** Iterate over all fields. */
  public Iterator<Entry<String, IndexField>> iterator() {
    return fields.entrySet().iterator();
  }

  public float getWeight() {
    return weight;
  }

  public void setWeight(float weight) {
    this.weight = weight;
  }

  public Metadata getDocumentMeta() {
    return documentMeta;
  }

  public void readFields(DataInput in) throws IOException {
    fields.clear();
    byte version = in.readByte();
    if (version != VERSION) {
      throw new VersionMismatchException(VERSION, version);
    }
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      String name = Text.readString(in);
      IndexField field = new IndexField();
      field.readFields(in);
      fields.put(name, field);
    }
    weight = in.readFloat();
    documentMeta.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);
    WritableUtils.writeVInt(out, fields.size());
    for (Map.Entry<String, IndexField> entry : fields.entrySet()) {
      Text.writeString(out, entry.getKey());
      IndexField field = entry.getValue();
      field.write(out);
    }
    out.writeFloat(weight);
    documentMeta.write(out);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("doc {\n");
    for (Map.Entry<String, IndexField> entry : fields.entrySet()) {
      sb.append("\t");
      sb.append(entry.getKey());
      sb.append(":\t");
      sb.append(format(entry.getValue()));
      sb.append("\n");
    }
    sb.append("}\n");
    return sb.toString();
  }

  public String formatAsLine() {
    StringBuilder sb = new StringBuilder();

    int i = 0;
    for (Map.Entry<String, IndexField> entry : fields.entrySet()) {
      if (i++ > 0) {
        sb.append(", ");
      }

      sb.append(entry.getKey());
      sb.append(" : ");
      sb.append(StringUtils.replaceChars(format(entry.getValue()), "[]", ""));
    }

    return sb.toString();
  }

  private String format(Object obj) {
    if (obj instanceof Date) {
      return DateTimeUtil.solrCompatibleFormat((Date)obj);
    }
    else {
      return obj.toString();
    }
  }

  public static class Builder {
    private static final Log LOG = LogFactory.getLog(new Object() {}.getClass().getEnclosingClass());

    private final IndexingFilters indexingFilters;
    private final ScoringFilters scoringFilters;

    public Builder(Configuration conf) {
      indexingFilters = new IndexingFilters(conf);
      scoringFilters = new ScoringFilters(conf);
    }

    /**
     * Index a {@link WebPage}, here we add the following fields:
     * <ol>
     * <li><tt>id</tt>: default uniqueKey for the {@link IndexDocument}.</li>
     * <li><tt>digest</tt>: Digest is used to identify pages (like unique ID)
     * and is used to remove duplicates during the dedup procedure. It is
     * calculated
     * <li><tt>batchId</tt>: The page belongs to a unique batchId, this is its
     * identifier.</li>
     * <li><tt>boost</tt>: Boost is used to calculate document (field) score
     * which can be used within queries submitted to the underlying indexing
     * library to find the best results. It's part of the scoring algorithms.
     * See scoring.link, scoring.opic, scoring.tld, etc.</li>
     * </ol>
     *
     * @param key
     *          The key of the page (reversed url).
     * @param page
     *          The {@link WebPage}.
     * @return The indexed document, or null if skipped by index indexingFilters.
     */
    public IndexDocument build(String key, WebPage page) {
      if (key == null || page == null) {
        return null;
      }

      IndexDocument doc = new IndexDocument(key);

      String url = doc.getUrl();

      try {
        doc = indexingFilters.filter(doc, url, page);
      } catch (IndexingException e) {
        LOG.warn("Error indexing " + key + ": " + e);
        return null;
      }

      // skip documents discarded by indexing indexingFilters
      if (doc == null) {
        return null;
      }

      doc.addIfAbsent("id", key);

      // TODO : we may not need digest
      if (page.getSignature() != null) {
        doc.add("digest", StringUtil.toHexString(page.getSignature()));
      }

      if (page.getBatchId() != null) {
        doc.add("batchId", page.getBatchId().toString());
      }

      float boost = 1.0f;
      // run scoring indexingFilters
      try {
        boost = scoringFilters.indexerScore(url, doc, page, boost);
      } catch (final ScoringFilterException e) {
        LOG.warn("Error calculating score " + key + ": " + e);
        return null;
      }

      doc.setWeight(boost);
      // store boost for use by explain and dedup
      doc.add("boost", Float.toString(boost));

      return doc;
    }
  }
}
