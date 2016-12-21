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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * A writable comparable container for an url with score. Provides a
 * {@link Partitioner} and {@link RawComparator} implementations for easy
 * integration with jobs.
 * 
 */
public final class GraphGroupKey implements WritableComparable<GraphGroupKey> {

  private static final Comparator<GraphGroupKey> comp = new GraphKeyComparator();
  private Text reversedUrl;
  private Text graphMode;
  private FloatWritable score;

  /**
   * Creates instance with empty url and zero score.
   */
  public GraphGroupKey() {
    reversedUrl = new Text();
    score = new FloatWritable();
  }

  public GraphGroupKey(String reversedUrl, float score) {
    this(reversedUrl, GraphMode.IN_LINK_GRAPH, score);
  }

  /**
   * Creates instance with provided non-writable types.
   * 
   * @param reversedUrl
   * @param score
   */
  public GraphGroupKey(String reversedUrl, GraphMode graphMode, float score) {
    this.reversedUrl = new Text(reversedUrl);
    this.graphMode = new Text(graphMode.name());
    this.score = new FloatWritable(score);
  }

  public void reset(String reversedUrl, float score) {
    this.reversedUrl = new Text(reversedUrl);
    this.score = new FloatWritable(score);
  }

  public String getReversedUrl() { return reversedUrl.toString(); }

  public void setReversedUrl(Text reversedUrl) {
    this.reversedUrl = reversedUrl;
  }

  public void setReversedUrl(String reversedUrl) {
    this.reversedUrl.set(reversedUrl);
  }

  public GraphMode getGraphMode() { return GraphMode.valueOf(graphMode.toString()); }

  public void setGraphMode(GraphMode graphMode) { this.graphMode = new Text(graphMode.name()); }

  public FloatWritable getScore() {
    return score;
  }

  public void setScore(FloatWritable score) {
    this.score = score;
  }

  public void setScore(float score) {
    this.score.set(score);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    reversedUrl.write(out);
    score.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    reversedUrl.readFields(in);
    score.readFields(in);
  }

  @Override
  public int compareTo(GraphGroupKey other) {
    return comp.compare(this, other);
  }

  @Override
  public String toString() {
    return "GraphGroupKey <" + reversedUrl + ", " + score + ">";
  }

  /**
   * A partitioner by {url}.
   */
  public static final class UrlOnlyPartitioner extends Partitioner<GraphGroupKey, Writable> {
    @Override
    public int getPartition(GraphGroupKey key, Writable val, int reduces) {
      return (key.reversedUrl.hashCode() & Integer.MAX_VALUE) % reduces;
    }
  }

  /**
   * Compares by {url,score}. Scores are sorted in descending order, that is
   * from high scores to low.
   */
  public static final class GraphKeyComparator implements RawComparator<GraphGroupKey> {
    private final WritableComparator textComp = new Text.Comparator();
    private final WritableComparator floatComp = new FloatWritable.Comparator();

    @Override
    public int compare(GraphGroupKey o1, GraphGroupKey o2) {
      int cmp = o1.getReversedUrl().compareTo(o2.getReversedUrl());
      if (cmp != 0) {
        return cmp;
      }
      // reverse order
      return -o1.getScore().compareTo(o2.getScore());
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        int deptLen1 = WritableUtils.decodeVIntSize(b1[s1])
            + WritableComparator.readVInt(b1, s1);
        int deptLen2 = WritableUtils.decodeVIntSize(b2[s2])
            + WritableComparator.readVInt(b2, s2);
        int cmp = textComp.compare(b1, s1, deptLen1, b2, s2, deptLen2);
        if (cmp != 0) {
          return cmp;
        }
        // reverse order
        return -floatComp.compare(b1, s1 + deptLen1, l1 - deptLen1, b2, s2
            + deptLen2, l2 - deptLen2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    /**
     * Compares by {url}.
     */
    public static final class UrlOnlyComparator implements RawComparator<GraphGroupKey> {
      private final WritableComparator textComp = new Text.Comparator();

      @Override
      public int compare(GraphGroupKey o1, GraphGroupKey o2) {
        return o1.getReversedUrl().compareTo(o2.getReversedUrl());
      }

      @Override
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
          int deptLen1 = WritableUtils.decodeVIntSize(b1[s1])
              + WritableComparator.readVInt(b1, s1);
          int deptLen2 = WritableUtils.decodeVIntSize(b2[s2])
              + WritableComparator.readVInt(b2, s2);
          return textComp.compare(b1, s1, deptLen1, b2, s2, deptLen2);
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      }
    }
  }
}