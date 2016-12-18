package org.apache.nutch.fetch;

import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.gora.ProtocolStatus;
import org.apache.nutch.storage.WebPage;

import java.time.Instant;

import static org.apache.nutch.storage.Mark.FETCH;
import static org.apache.nutch.storage.Mark.GENERATE;
import static org.apache.nutch.storage.Mark.INDEX;

/**
 * Created by vincent on 16-9-10.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class FetchUtil {

  static public void updateStatus(WebPage page, byte status) {
    updateStatus(page, status, null);
  }

  static public void updateStatus(WebPage page, byte status, ProtocolStatus pstatus) {
    page.setStatus((int) status);
    if (pstatus != null) {
      page.setProtocolStatus(pstatus);
    }

    page.increaseFetchCount();
  }

  static public void updateMarks(WebPage page) {
    page.putMarkIfNonNull(FETCH, page.getMark(GENERATE));
    page.removeMark(INDEX);
  }

  static public void updateContent(WebPage page, Content content) {
    updateContent(page, content, null);
  }

  static public void updateContent(WebPage page, Content content, String contentType) {
    if (content == null) {
      return;
    }

    // Content is added to page here for ParseUtil be able to parse it.
//    page.setBaseUrl(new Utf8(content.getBaseUrl()));
    page.setBaseUrl(content.getBaseUrl());
//    page.setContent(ByteBuffer.wrap(content.getContent()));
    page.setContent(content.getContent());

    if (contentType != null) {
      content.setContentType(contentType);
    } else {
      contentType = content.getContentType();
    }

    if (contentType != null) {
      page.setContentType(contentType);
      // LOG.error("Failed to determine content type!");
    }
  }

  static public void updateFetchTime(WebPage page) {
    Instant prevFetchTime = page.getFetchTime();
    Instant fetchTime = Instant.now();

    page.setPrevFetchTime(prevFetchTime);
    page.setFetchTime(fetchTime);

    page.putFetchTimeHistory(fetchTime);
  }
}
