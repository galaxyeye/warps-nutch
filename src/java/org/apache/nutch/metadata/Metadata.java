/*
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
package org.apache.nutch.metadata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A multi-valued metadata container.
 *
 * @author Chris Mattmann
 * @author J&eacute;r&ocirc;me Charron
 *
 */
public class Metadata implements Writable, DublinCore, HttpHeaders, Nutch {

  public enum Name {
    UNKNOWN(""),

    IS_NAVIGATOR("I_N"),
    IS_SEED("I_S"),
    IS_DETAIL("I_D"),

    /** generate */
    GENERATE_TIME("G_GT"),
    DISTANCE("G_D"),

    /** fetch */
    FETCH_TIME_HISTORY("F_FTH"),
    FETCH_COUNT("F_FC"),
    FETCH_PRIORITY("F_FP"),

    REDIRECT_DISCOVERED("RD"),

    /** parse */
    CONTENT_TITLE("P_CT"),
    TEXT_CONTENT("P_TC"),
    HTML_CONTENT("P_HC"),
    TOTAL_OUT_LINKS("P_OLC"),
    OLD_OUT_LINKS("P_AOL"),

    /** index */
    INDEX_TIME_HISTORY("ITH"),

    /** score */
    ARTICLE_SCORE("S_AS"),
    CASH_KEY("S_CASH"),

    /** content */
    ORIGINAL_CHAR_ENCODING("C_OCE"),
    CHAR_ENCODING_FOR_CONVERSION("C_CEFC"),
    PAGE_CATEGORY("C_PC"),
    PAGE_CATEGORY_LIKELIHOOD("C_PCL"),
    TEXT_CONTENT_LENGTH("C_TCL"),
    REFERRER("C_R"),
    REFERRED_ARTICLES("C_RA"),
    REFERRED_CHARS("C_RC"),
    REFERRED_PUBLISH_TIME("C_RPT"),
    PREV_REFERRED_PUBLISH_TIME("C_PRPT"),
    PUBLISH_TIME("C_PT"),
    PREV_PUBLISH_TIME("C_PPT"),

    META_KEYWORDS("meta_keywords"),
    META_DESCRIPTION("meta_description"),

    /** tmp */
    TMP_PAGE_FROM_SEED("T_PFS"),
    TMP_IS_DETAIL("T_ID"),
    TMP_CHARS("T_C");

    private String text;

    Name(String name) {
      this.text = name;
    }

    public String text() {
      return this.text;
    }

    public static Name of(String name) {
      switch (name) {
        case "I_N":
          return IS_NAVIGATOR;
        case "I_S":
          return IS_SEED;
        case "I_D":
          return IS_DETAIL;

        /** generate */
        case "G_GT":
          return GENERATE_TIME;
        case "G_D":
          return DISTANCE;

        /** fetch */
        case "F_FTH":
          return FETCH_TIME_HISTORY;
        case "F_FC":
          return FETCH_COUNT;
        case "F_FP":
          return FETCH_PRIORITY;

        case "RD":
          return REDIRECT_DISCOVERED;

        case "P_CT":
          return CONTENT_TITLE;
        case "P_TC":
          return TEXT_CONTENT;
        case "P_HC":
          return HTML_CONTENT;
        case "P_OLC":
          return TOTAL_OUT_LINKS;
        case "P_AOL":
          return OLD_OUT_LINKS;

        /** index */
        case "ITH":
          return INDEX_TIME_HISTORY;

        /** score */
        case "S_AS":
          return ARTICLE_SCORE;
        case "S_CASH":
          return CASH_KEY;

        /** content */
        case "C_OCE":
          return ORIGINAL_CHAR_ENCODING;
        case "C_CEFC":
          return CHAR_ENCODING_FOR_CONVERSION;
        case "C_PC":
          return PAGE_CATEGORY;
        case "C_PCL":
          return PAGE_CATEGORY_LIKELIHOOD;
        case "C_TCL":
          return TEXT_CONTENT_LENGTH;
        case "C_R":
          return REFERRER;
        case "C_RA":
          return REFERRED_ARTICLES;
        case "C_RC":
          return REFERRED_CHARS;
        case "C_RPT":
          return REFERRED_PUBLISH_TIME;
        case "C_PRPT":
          return PREV_REFERRED_PUBLISH_TIME;
        case "C_PT":
          return PUBLISH_TIME;
        case "C_PPT":
          return PREV_PUBLISH_TIME;

        case "meta_keywords":
          return META_KEYWORDS;
        case "meta_description":
          return META_DESCRIPTION;
        default:
          return UNKNOWN;
      }
    }
  }

  public static final String META_TMP = "TMP_";

  /**
   * A map of all metadata attributes.
   */
  private Map<String, String[]> metadata = new HashMap<>();

  /**
   * Constructs a new, empty metadata.
   */
  public Metadata() {
  }

  /**
   * Returns true if named value is multivalued.
   *
   * @param name name of metadata
   * @return true is named value is multivalued, false if single value or null
   */
  public boolean isMultiValued(final String name) {
    return metadata.get(name) != null && metadata.get(name).length > 1;
  }

  /**
   * Returns an array of the names contained in the metadata.
   *
   * @return Metadata names
   */
  public String[] names() {
    return metadata.keySet().toArray(new String[metadata.keySet().size()]);
  }

  /**
   * Get the value associated to a metadata name. If many values are assiociated
   * to the specified name, then the first one is returned.
   *
   * @param name of the metadata.
   * @return the value associated to the specified metadata name.
   */
  public String get(final String name) {
    String[] values = metadata.get(name);
    if (values == null) {
      return null;
    } else {
      return values[0];
    }
  }

  public String get(final Name name) {
    return get(name.text());
  }

  /**
   * Get the values associated to a metadata name.
   *
   * @param name of the metadata.
   * @return the values associated to a metadata name.
   */
  public String[] getValues(final String name) {
    return _getValues(name);
  }

  private String[] _getValues(final String name) {
    String[] values = metadata.get(name);
    if (values == null) {
      values = new String[0];
    }
    return values;
  }

  /**
   * Add a metadata name/value mapping. Add the specified value to the list of
   * values associated to the specified metadata name.
   *
   * @param name  the metadata name.
   * @param value the metadata value.
   */
  public void add(final String name, final String value) {
    String[] values = metadata.get(name);
    if (values == null) {
      set(name, value);
    } else {
      String[] newValues = new String[values.length + 1];
      System.arraycopy(values, 0, newValues, 0, values.length);
      newValues[newValues.length - 1] = value;
      metadata.put(name, newValues);
    }
  }

  public void add(final Name name, final String value) { add(name.text(), value); }

  public void add(Name name, int value) { add(name, String.valueOf(value)); }

  public void add(Name name, long value) { add(name, String.valueOf(value)); }

  public void add(Name name, Instant value) { add(name, DateTimeUtil.isoInstantFormat(value)); }

  public int getInteger(Name name, int defaultValue) {
    String s = get(name.text());
    return StringUtil.tryParseInt(s, defaultValue);
  }

  public long getLong(Name name, long defaultValue) {
    String s = get(name.text());
    return StringUtil.tryParseLong(s, defaultValue);
  }

  public boolean getBoolean(Metadata.Name name, Boolean defaultValue) {
    String s = get(name);
    if (s == null) {
      return defaultValue;
    }
    return Boolean.valueOf(s);
  }

  public Instant getInstant(Metadata.Name name, Instant defaultValue) {
    return DateTimeUtil.parseInstant(get(name), defaultValue);
  }

  /**
   * Copy All key-value pairs from properties.
   *
   * @param properties properties to copy from
   */
  public void setAll(Properties properties) {
    Enumeration<?> names = properties.propertyNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      metadata.put(name, new String[]{properties.getProperty(name)});
    }
  }

  /**
   * Set metadata name/value. Associate the specified value to the specified
   * metadata name. If some previous values were associated to this name, they
   * are removed.
   *
   * @param name  the metadata name.
   * @param value the metadata value.
   */
  public void set(String name, String value) {
    metadata.put(name, new String[]{value});
  }

  public void set(Name name, String value) {
    set(name.text(), value);
  }

  public void set(Name name, int value) { set(name, String.valueOf(value)); }

  public void set(Name name, long value) { set(name, String.valueOf(value)); }

  public void set(Name name, Instant value) { set(name, DateTimeUtil.isoInstantFormat(value)); }

  /**
   * Remove a metadata and all its associated values.
   *
   * @param name
   *          metadata name to remove
   */
  public void remove(String name) {
    metadata.remove(name);
  }

  /**
   * Returns the number of metadata names in this metadata.
   *
   * @return number of metadata names
   */
  public int size() {
    return metadata.size();
  }

  /** Remove all mappings from metadata. */
  public void clear() {
    metadata.clear();
  }

  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    Metadata other = null;
    try {
      other = (Metadata) o;
    } catch (ClassCastException cce) {
      return false;
    }

    if (other.size() != size()) {
      return false;
    }

    String[] names = names();
    for (int i = 0; i < names.length; i++) {
      String[] otherValues = other._getValues(names[i]);
      String[] thisValues = _getValues(names[i]);
      if (otherValues.length != thisValues.length) {
        return false;
      }
      for (int j = 0; j < otherValues.length; j++) {
        if (!otherValues[j].equals(thisValues[j])) {
          return false;
        }
      }
    }
    return true;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    String[] names = names();
    for (int i = 0; i < names.length; i++) {
      String[] values = _getValues(names[i]);
      for (int j = 0; j < values.length; j++) {
        buf.append(names[i]).append("=").append(values[j]).append(" ");
      }
    }
    return buf.toString();
  }

  public final void write(DataOutput out) throws IOException {
    out.writeInt(size());
    String[] values = null;
    String[] names = names();
    for (int i = 0; i < names.length; i++) {
      Text.writeString(out, names[i]);
      values = _getValues(names[i]);
      int cnt = 0;
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null)
          cnt++;
      }
      out.writeInt(cnt);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          Text.writeString(out, values[j]);
        }
      }
    }
  }

  public final void readFields(DataInput in) throws IOException {
    int keySize = in.readInt();
    String key;
    for (int i = 0; i < keySize; i++) {
      key = Text.readString(in);
      int valueSize = in.readInt();
      for (int j = 0; j < valueSize; j++) {
        add(key, Text.readString(in));
      }
    }
  }
}
