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
package org.apache.nutch.parse;

public interface ParseStatusCodes {

  String[] majorCodes = { "notparsed", "success", "failed" };

  // Primary status codes:

  /** Parsing was not performed. */
  byte NOTPARSED = 0;

  /** Parsing succeeded. */
  byte SUCCESS = 1;

  /** General failure. There may be a more specific error message in arguments. */
  byte FAILED = 2;

  // Secondary success codes go here:

  short SUCCESS_OK = 0;

  /**
   * Parsed content contains a directive to redirect to another URL. The target
   * URL can be retrieved from the arguments.
   */
  short SUCCESS_REDIRECT = 100;

  // Secondary failure codes go here:

  /**
   * Parsing failed. An Exception occured (which may be retrieved from the
   * arguments).
   */
  short FAILED_EXCEPTION = 200;
  /**
   * Parsing failed. Content was truncated, but the parser cannot handle
   * incomplete content.
   */
  short FAILED_TRUNCATED = 202;
  /**
   * Parsing failed. Invalid format - the content may be corrupted or of wrong
   * type.
   */
  short FAILED_INVALID_FORMAT = 203;
  /**
   * Parsing failed. Other related parts of the content are needed to halt
   * parsing. The list of URLs to missing parts may be provided in arguments.
   * The SimpleFetcher may decide to fetch these parts at once, then put them into
   * Content.metadata, and supply them for re-parsing.
   */
  short FAILED_MISSING_PARTS = 204;
  /**
   * Parsing failed. There was no content to be parsed - probably caused by
   * errors at protocol stage.
   */
  short FAILED_MISSING_CONTENT = 205;
}
