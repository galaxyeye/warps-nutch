package org.apache.nutch.persist.graph;

import org.apache.gora.util.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;

/**
 * Created by vincent on 16-12-21.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class Vertex implements Writable, Comparable<Vertex> {
  private Configuration conf;
  private String url;
  private String anchor;
  private WebPage page;
  private int depth;
  private Metadata metadata = new Metadata();

  public Vertex(Configuration conf) {
    this.conf = conf;
  }

  public Vertex(String url, String anchor, WebPage page, int depth, Configuration conf) {
    this.url = url;
    this.anchor = anchor;
    this.page = page;
    this.depth = depth;
    this.conf = conf;
  }

  public void reset(String url, String anchor, WebPage page, int depth) {
    this.url = url;
    this.anchor = anchor;
    this.page = page;
    this.depth = depth;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setAnchor(String anchor) { this.anchor = anchor; }

  public String getAnchor() { return anchor; }

  public WebPage getWebPage() { return page; }

  public void setWebPage(WebPage page) { this.page = page; }

  public int getDepth() { return depth; }

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
  public void write(DataOutput output) throws IOException {
    Text.writeString(output, url);
    IOUtils.serialize(conf, output, page.get(), GoraWebPage.class);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    url = Text.readString(input);
    page = WebPage.wrap(IOUtils.deserialize(conf, input, page.get(), GoraWebPage.class));
  }

  @Override
  public boolean equals(Object vertex) {
    if (!(vertex instanceof Vertex)) {
      return false;
    }

    return url.equals(((Vertex) vertex).url);
  }

  @Override
  public int compareTo(Vertex vertex) {
    if (vertex == null) {
      return -1;
    }

    return url.compareTo(vertex.url);
  }

  @Override
  public String toString() {
    return url;
  }
}
