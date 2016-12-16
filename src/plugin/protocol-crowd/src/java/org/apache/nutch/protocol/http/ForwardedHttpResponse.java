/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.protocol.http;

// JDK imports

import org.apache.avro.util.Utf8;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.storage.WebPage;

import java.net.URL;

/** An HTTP response. */
public class ForwardedHttpResponse implements Response {

  private final URL url;
  private byte[] content;
  private int code;
  private Metadata headers;

  public ForwardedHttpResponse(URL url, byte[] content, int code, Metadata headers, WebPage page) {
    this.url = url;
    this.content = content;
    this.code = code;
    this.headers = headers;

    // add headers in metadata to row
    if (page.getHeaders() != null) {
      page.getHeaders().clear();
    }

    for (String key : headers.names()) {
      page.getHeaders().put(new Utf8(key), new Utf8(headers.get(key)));
    }
  }

  /** Returns the URL used to retrieve this response. */
  @Override
  public URL getUrl() {
    return url;
  }

  /** Returns the response code. */
  @Override
  public int getCode() {
    return code;
  }

  /** Returns the value of a named header. */
  @Override
  public String getHeader(String name) {
    return headers.get(name);
  }

  /** Returns all the headers. */
  @Override
  public Metadata getHeaders() {
    return headers;
  }

  /** Returns the full content of the response. */
  @Override
  public byte[] getContent() {
    return content;
  }

}
