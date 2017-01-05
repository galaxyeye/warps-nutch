package org.apache.nutch.fetch;

import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.ProtocolStatus;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchUtil;

import java.time.Instant;

import static org.apache.nutch.persist.Mark.FETCH;
import static org.apache.nutch.persist.Mark.GENERATE;

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
  }

  static public void updateContent(WebPage page, Content content) { updateContent(page, content, null); }

  static public void updateContent(WebPage page, Content content, String contentType) {
    if (content == null) {
      return;
    }

    page.setBaseUrl(content.getBaseUrl());
    page.setContent(content.getContent());

    if (contentType != null) {
      content.setContentType(contentType);
    } else {
      contentType = content.getContentType();
    }

    if (contentType != null) {
      page.setContentType(contentType);
    }
    else {
      NutchUtil.LOG.error("Failed to determine content type!");
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
