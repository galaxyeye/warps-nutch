package org.apache.nutch.fetch;

import org.apache.avro.util.Utf8;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;

import java.nio.ByteBuffer;

/**
 * Created by vincent on 16-9-10.
 */
public class FetchUtil {

  static public void setStatus(WebPage page, byte status) {
    setStatus(page, status, null);
  }

  static public void setStatus(WebPage page, byte status, ProtocolStatus pstatus) {
    page.setStatus((int) status);
    if (pstatus != null) {
      page.setProtocolStatus(pstatus);
    }
  }

  static public void setMarks(WebPage page) {
    /** Set fetch mark */
    Mark.FETCH_MARK.putMark(page, Mark.GENERATE_MARK.checkMark(page));
    /** Unset index mark if exist, this page should be re-indexed */
    Mark.INDEX_MARK.removeMarkIfExist(page);
  }

  static public void setContent(WebPage page, Content content) {
    setContent(page, content, null);
  }

  static public void setContent(WebPage page, Content content, String contentType) {
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

  static public void setFetchTime(WebPage page) {
    final long prevFetchTime = page.getFetchTime();
    final long fetchTime = System.currentTimeMillis();
    page.setPrevFetchTime(prevFetchTime);
    page.setFetchTime(fetchTime);

    TableUtil.putFetchTimeHistory(page, fetchTime);
  }
}
