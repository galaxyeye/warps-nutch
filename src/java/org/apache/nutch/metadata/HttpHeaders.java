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
package org.apache.nutch.metadata;

/**
 * A collection of HTTP header names.
 * 
 * @see <a href="http://rfc-ref.org/RFC-TEXTS/2616/">Hypertext Transfer Protocol
 *      -- HTTP/1.1 (RFC 2616)</a>
 * 
 * @author Chris Mattmann
 * @author J&eacute;r&ocirc;me Charron
 */
public interface HttpHeaders {
  public final static String TRANSFER_ENCODING = "Transfer-Encoding";

  public final static String CONTENT_ENCODING = "Content-Encoding";

  public final static String CONTENT_LANGUAGE = "Content-Language";

  public final static String CONTENT_LENGTH = "Content-Length";

  public final static String CONTENT_LOCATION = "Content-Location";

  public static final String CONTENT_DISPOSITION = "Content-Disposition";

  public final static String CONTENT_MD5 = "Content-MD5";

  public final static String CONTENT_TYPE = "Content-Type";

  public final static String LAST_MODIFIED = "Last-Modified";
  
  public final static String LOCATION = "Location";

  // For satellite information
  public final static String Q_VERSION = "Q-Version";

  public final static String Q_USERNAME = "Q-Username";

  public final static String Q_PASSWORD = "Q-Password";

  public final static String Q_JOB_ID = "Q-Job-Id";

  public final static String Q_QUEUE_ID = "Q-Queue-Id";

  public final static String Q_ITEM_ID = "Q-Item-Id";

  public final static String Q_RESPONSE_TIME = "Q-Response-Time";

  public final static String Q_STATUS_CODE = "Q-Status-Code";

  public final static String Q_CHECKSUM = "Q-Checksum";

  public final static String Q_URL = "Q-Url";
}
