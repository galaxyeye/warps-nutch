package org.apache.nutch.fetch;

import org.apache.avro.util.Utf8;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;

import java.nio.ByteBuffer;

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

    TableUtil.increaseFetchTimes(page);
  }

  static public void updateMarks(WebPage page) {
    /** Set fetch mark */
    Mark.FETCH_MARK.putMark(page, Mark.GENERATE_MARK.checkMark(page));
    /** Unset index mark if exist, this page should be re-indexed */
    Mark.INDEX_MARK.removeMarkIfExist(page);
  }

  static public void updateContent(WebPage page, Content content) {
    updateContent(page, content, null);
  }

  static public void updateContent(WebPage page, Content content, String contentType) {
    if (content == null) {
      return;
    }

    // Content is added to page here for ParseUtil be able to parse it.
    page.setBaseUrl(new Utf8(content.getBaseUrl()));
    page.setContent(ByteBuffer.wrap(content.getContent()));

    if (contentType != null) {
      content.setContentType(contentType);
    } else {
      contentType = content.getContentType();
    }

    if (contentType != null) {
      page.setContentType(new Utf8(contentType));
      // LOG.error("Failed to determine content type!");
    }
  }

  static public void updateFetchTime(WebPage page, byte status) {
    final long prevFetchTime = page.getFetchTime();
    final long fetchTime = System.currentTimeMillis();
    page.setPrevFetchTime(prevFetchTime);
    page.setFetchTime(fetchTime);

    if (status == CrawlStatus.STATUS_FETCHED) {
      TableUtil.putFetchTimeHistory(page, fetchTime);
    }
  }
}
