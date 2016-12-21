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
package org.apache.nutch.scoring;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ScoreDatum implements Writable {

  private float score;
  private String url;
  private String anchor;
  private int distance;
  private Metadata metadata = new Metadata();

  public ScoreDatum() {
  }

  public ScoreDatum(float score, Outlink outlink, int depth) {
    this(score, outlink.getToUrl(), outlink.getAnchor(), depth);
  }

  public ScoreDatum(float score, CharSequence url, CharSequence anchor, int depth) {
    this(score, url.toString(), anchor.toString(), depth);
  }

  public ScoreDatum(float score, String url, String anchor, int depth) {
    this.score = score;
    this.url = url;
    this.anchor = anchor;
    this.distance = depth;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    score = in.readFloat();
    url = Text.readString(in);
    anchor = Text.readString(in);
    distance = WritableUtils.readVInt(in);
    metadata.clear();
    metadata.readFields(in);

//    int size = WritableUtils.readVInt(in);
//    for (int i = 0; i < size; i++) {
//      String key = Text.readString(in);
//      byte[] value = Bytes.readByteArray(in);
//      metadata.put(key, ByteBuffer.wrap(value));
//    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(score);
    Text.writeString(out, url);
    Text.writeString(out, anchor);
    WritableUtils.writeVInt(out, distance);
    metadata.write(out);

//    WritableUtils.writeVInt(out, metadata.size());
//    for (Entry<CharSequence, ByteBuffer> e : metadata.entrySet()) {
//      Text.writeString(out, e.getKey());
//      Bytes.writeByteArray(out, e.getValue().array());
//    }
  }

//  public byte[] getMetadata(String key) {
//    return metadata.get(key);
//  }
//
//  public void setMetadata(String key, byte[] value) {
//    metadata.put(key, value);
//  }
//
//  public void setMetadata(Metadata.Name key, byte[] value) {
//    metadata.put(key.value(), value);
//  }
//
//  public void setMetadata(Metadata.Name key, ByteBuffer value) {
//    setMetadata(key, value.array());
//  }
//
//  public Instant getRefPublishTime() {
//
//  }

  public float getScore() {
    return score;
  }

  public void setScore(float score) {
    this.score = score;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getAnchor() {
    return anchor;
  }

  public int getDistance() {
    return distance;
  }

  public void addMetadata(Metadata.Name name, String value) {
    metadata.add(name, value);
  }

  public void addMetadata(Metadata.Name name, long value) {
    metadata.add(name, String.valueOf(value));
  }

  public void addMetadata(Metadata.Name name, Instant value) {
    metadata.add(name, DateTimeUtil.solrCompatibleFormat(value));
  }

  public String getMetadata(Metadata.Name name) {
    return metadata.get(name);
  }

  public long getMetadata(Metadata.Name name, long defaultValue) {
    String s = metadata.get(name);

    return StringUtil.tryParseLong(s, defaultValue);
  }

  public Instant getMetadata(Metadata.Name name, Instant defaultValue) {
    String s = metadata.get(name);

    return DateTimeUtil.parseTime(s, defaultValue);
  }

  @Override
  public String toString() {
    return "ScoreDatum [score=" + score + ", url=" + url + ", anchor=" + anchor
        + ", distance=" + distance + ", metadata=" + metadata + "]";
  }

}
