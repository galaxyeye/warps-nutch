package org.apache.nutch.filter;

/**
 * Created by vincent on 16-12-18.
 */
public enum PageCategory {
  INDEX, DETAIL, SEARCH, MEDIA, BBS, TIEBA, BLOG, UNKNOWN;

  public boolean is(PageCategory pageCategory) {
    return pageCategory == this;
  }
  public boolean isIndex() {
    return this == INDEX;
  }
  public boolean isDetail() {
    return this == DETAIL;
  }
  public boolean isSearch() {
    return this == SEARCH;
  }
  public boolean isMedia() {
    return this == MEDIA;
  }
  public boolean isBBS() {
    return this == BBS;
  }
  public boolean isTieBa() {
    return this == TIEBA;
  }
  public boolean isBlog() {
    return this == BLOG;
  }
  public boolean isUnknown() {
    return this == UNKNOWN;
  }
}
