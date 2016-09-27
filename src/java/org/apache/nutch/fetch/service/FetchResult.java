package org.apache.nutch.fetch.service;

import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.StringUtil;

/**
 *
 * */
public class FetchResult implements HttpHeaders {

  private Metadata headers;
  private byte[] content;

  public FetchResult(Metadata headers, byte[] content) {
    this.headers = headers;
    this.content = content;
  }

  public Metadata getHeaders() {
    return headers;
  }

  public void setHeaders(Metadata headers) {
    this.headers = headers;
  }

  public byte[] getContent() {
    return content;
  }

  public void setContent(byte[] content) {
    this.content = content;
  }

  public String getHeader(String name) {
    return headers.get(name);
  }

  public String getUrl() {
    return headers.get(Q_URL);
  }

  public int getJobId() {
    return StringUtil.tryParseInt(headers.get(Q_JOB_ID));
  }

  public String getQueueId() {
    return headers.get(Q_QUEUE_ID);
  }

  public int getItemId() {
    return StringUtil.tryParseInt(headers.get(Q_ITEM_ID));
  }

  public int getStatusCode() {
    return StringUtil.tryParseInt(headers.get(Q_STATUS_CODE));
  }
}
