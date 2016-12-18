package org.apache.nutch.filter;

import org.jetbrains.annotations.Contract;

/**
 * Created by vincent on 16-12-18.
 */
public enum PageCategory {
  INDEX, DETAIL, SEARCH, MEDIA, BBS, TIEBA, BLOG, UNKNOWN;

  @Contract(pure = true)
  public boolean is(PageCategory pageCategory) {
    return pageCategory == this;
  }

  @Contract(pure = true)
  public boolean isIndex() {
    return this == INDEX;
  }

  @Contract(pure = true)
  public boolean isDetail() {
    return this == DETAIL;
  }

  @Contract(pure = true)
  public boolean isSearch() {
    return this == SEARCH;
  }

  @Contract(pure = true)
  public boolean isMedia() {
    return this == MEDIA;
  }

  @Contract(pure = true)
  public boolean isBBS() {
    return this == BBS;
  }

  @Contract(pure = true)
  public boolean isTieBa() {
    return this == TIEBA;
  }

  @Contract(pure = true)
  public boolean isBlog() {
    return this == BLOG;
  }

  @Contract(pure = true)
  public boolean isUnknown() {
    return this == UNKNOWN;
  }
}
