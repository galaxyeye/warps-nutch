package org.apache.nutch.graph;

import org.apache.nutch.persist.WebPage;

import java.time.Instant;

/**
 * Created by vincent on 16-12-29.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class WebVertex {
  private String url;
  private WebPage page;
  private int depth;
  private String anchor;
  private Instant publishTime;
  private Instant referredPublishTime;
  private int referredArticles;
  private int referredChars;

  public WebVertex(String url, WebPage page, int depth) {
    this(url, "", page, depth);
  }

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

  public boolean hasWebPage() { return this.page != null; }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) { this.depth = depth; }

  public String getAnchor() { return anchor; }

  public void setAnchor(String anchor) { this.anchor = anchor; }

  public WebPage getPage() { return page; }

  public void setPage(WebPage page) { this.page = page; }

  public Instant getPublishTime() { return publishTime; }

  public void setPublishTime(Instant publishTime) { this.publishTime = publishTime; }

  public Instant getReferredPublishTime() { return referredPublishTime; }

  public void setReferredPublishTime(Instant referredPublishTime) { this.referredPublishTime = referredPublishTime; }

  public int getReferredArticles() { return referredArticles; }

  public void setReferredArticles(int referredArticles) { this.referredArticles = referredArticles; }

  public int getReferredChars() { return referredChars; }

  public void setReferredChars(int referredChars) { this.referredChars = referredChars; }

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
