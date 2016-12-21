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
package org.apache.nutch.persist.graph;

import org.apache.hadoop.io.Writable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;

public class Edge implements Writable {

  private Vertex v1;
  private Vertex v2;
  private float score = 0.0f;
  private Metadata metadata = new Metadata();

  public Edge() {
  }

  public Edge(Vertex v1, Vertex v2) {
    this.v1 = v1;
    this.v2 = v2;
  }

  public Edge(Vertex v1, Vertex v2, float score) {
    this.v1 = v1;
    this.v2 = v2;
    this.score = score;
  }

  public Vertex getV1() { return v1; }

  public void setV1(Vertex vertex) { v1 = vertex;}

  public Vertex getV2() { return v2; }

  public void setV2(Vertex vertex) { v2 = vertex;}

  public float getScore() { return score; }

  public void setScore(float score) { this.score = score; }

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
  public void readFields(DataInput in) throws IOException {
//    sourceUrl = Text.readString(in);
//    destUrl = Text.readString(in);
//    score = in.readFloat();
//    distance = WritableUtils.readVInt(in);
    metadata.clear();
    metadata.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
//    Text.writeString(out, sourceUrl);
//    Text.writeString(out, destUrl);
//    out.writeFloat(score);
//    WritableUtils.writeVInt(out, distance);
    metadata.write(out);
  }

  @Override
  public String toString() {
    return String.format("%s -[%f]-> %s", v1, score, v2);
  }
}
