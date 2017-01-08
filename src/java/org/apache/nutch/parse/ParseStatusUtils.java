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

import com.google.common.collect.Lists;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.persist.gora.ParseStatus;
import org.apache.nutch.util.TableUtil;

import java.util.HashMap;
import java.util.List;

import static org.apache.nutch.parse.ParseStatusCodes.*;

public class ParseStatusUtils {

  public static final ParseStatus STATUS_SUCCESS = ParseStatus.newBuilder().build();
  public static final HashMap<Short, String> minorCodes = new HashMap<>();

  static {
    STATUS_SUCCESS.setMajorCode((int) SUCCESS);
    minorCodes.put(SUCCESS_OK, "ok");
    minorCodes.put(SUCCESS_REDIRECT, "redirect");
    minorCodes.put(FAILED_EXCEPTION, "exception");
    minorCodes.put(FAILED_INVALID_FORMAT, "invalid_format");
    minorCodes.put(FAILED_MISSING_CONTENT, "missing_content");
    minorCodes.put(FAILED_MISSING_PARTS, "missing_parts");
    minorCodes.put(FAILED_TRUNCATED, "truncated");
  }

  public static boolean isSuccess(ParseStatus status) {
    return status != null && status.getMajorCode() == SUCCESS;
  }

  /**
   * A convenience method. Return a String representation of the first argument,
   * or null.
   */
  public static String getMessage(ParseStatus status) {
    List<CharSequence> args = status.getArgs();
    if (args != null && args.size() > 0) {
      return TableUtil.toString(args.iterator().next());
    }
    return null;
  }

  public static String getArg(ParseStatus status, int n) {
    List<CharSequence> args = status.getArgs();
    if (args == null) {
      return null;
    }
    int i = 0;
    for (CharSequence arg : args) {
      if (i == n) {
        return TableUtil.toString(arg);
      }
      i++;
    }
    return null;
  }

  public static ParseResult getEmptyParse(Exception e, Configuration conf) {
    ParseStatus status = ParseStatus.newBuilder().build();
    status.setMajorCode((int) FAILED);
    status.setMinorCode((int) FAILED_EXCEPTION);

    if (e != null) {
      status.getArgs().add(new Utf8(e.toString()));
    }

    return new ParseResult("", "", Lists.newArrayList(), status);
  }

  public static ParseResult getEmptyParse(int minorCode, String message, Configuration conf) {
    ParseStatus status = ParseStatus.newBuilder().build();
    status.setMajorCode((int) FAILED);
    status.setMinorCode(minorCode);
    status.getArgs().add(new Utf8(message));

    return new ParseResult("", "", Lists.newArrayList(), status);
  }

  public static String toString(ParseStatus status) {
    if (status == null) {
      return "(null)";
    }

    return majorCodes[status.getMajorCode()] + "/" + minorCodes.get(status.getMinorCode().shortValue()) +
        " (" + status.getMajorCode() + "/" + status.getMinorCode() + ")" +
        ", args=[" + StringUtils.join(status.getArgs(), ',') + "]";
  }
}
