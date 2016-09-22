/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.protocol;

public interface ProtocolStatusCodes {

  /** Content was retrieved without errors. */
  int SUCCESS = 1;
  /** Content was not retrieved. Any further errors may be indicated in args. */
  int FAILED = 2;

  /** This protocol was not found. Application may attempt to retry later. */
  int PROTO_NOT_FOUND = 10;
  /** Resource is gone. */
  int GONE = 11;
  /** Resource has moved permanently. New url should be found in args. */
  int MOVED = 12;
  /** Resource has moved temporarily. New url should be found in args. */
  int TEMP_MOVED = 13;
  /** Resource was not found. */
  int NOTFOUND = 14;
  /** Temporary failure. Application may retry immediately. */
  int RETRY = 15;
  /**
   * Unspecified exception occured. Further information may be provided in args.
   */
  int EXCEPTION = 16;
  /** Access denied - authorization required, but missing/incorrect. */
  int ACCESS_DENIED = 17;
  /** Access denied by robots.txt rules. */
  int ROBOTS_DENIED = 18;
  /** Too many redirects. */
  int REDIR_EXCEEDED = 19;
  /** Not fetching. */
  int NOTFETCHING = 20;
  /** Unchanged since the last fetch. */
  int NOTMODIFIED = 21;
  /**
   * Request was refused by protocol plugins, because it would block. The
   * expected number of milliseconds to wait before retry may be provided in
   * args.
   */
  int WOULDBLOCK = 22;
  /** Thread was blocked http.max.delays times during fetching. */
  int BLOCKED = 23;
  /** Failed to find the target host. */
  int UNKNOWN_HOST = 24;
  /** Find the target host timed out. */
  int CONNECTION_TIMED_OUT = 25;
}
