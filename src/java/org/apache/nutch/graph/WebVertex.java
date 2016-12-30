package org.apache.nutch.graph;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.persist.WebPage;

import java.time.Instant;

/**
 * Created by vincent on 16-12-29.
 */
public class WebVertex {
  private String url;
  private WebPage page;
  private int depth;
  private String anchor;
  private Instant publishTime;

  public WebVertex(String url, String anchor, WebPage page, int depth) {
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

  public WebPage getWebPage() {
    return page;
  }

  public void setWebPage(WebPage page) {
    this.page = page;
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) { this.depth = depth; }

  public String getAnchor() { return anchor; }

  public void setAnchor(String anchor) { this.anchor = anchor; }

  @Override
  public boolean equals(Object vertex) {
    if (!(vertex instanceof WebVertex)) {
      return false;
    }

    return url.equals(((WebVertex) vertex).url);
  }

  @Override
  public int hashCode() {
    return (url == null) ? 0 : url.hashCode();
  }

  @Override
  public String toString() {
    return url;
  }
}
