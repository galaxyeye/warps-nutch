package org.apache.nutch.graph;

import org.apache.nutch.persist.WebPage;

/**
 * Created by vincent on 16-12-29.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class WebVertex {
  private String url;
  private WebPage page;

  public WebVertex(CharSequence url) {
    this(url.toString(), null);
  }

  public WebVertex(CharSequence url, WebPage page) {
    this.url = url.toString();
    this.page = page;
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
