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

package org.apache.nutch.util;

import org.apache.commons.lang.time.DateUtils;
import org.apache.nutch.net.protocols.HttpDateFormat;

import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * TODO : use apache's DateUtils, DateFormatUtils
 * */
public class TimingUtil {

  // 2016-03-05 20:07:51
  // TODO : What's the difference between HH and hh? Guess : 24 hours VS 12 hours
  public static String[] GENERAL_DATE_TIME_FORMATS = new String[]{
      "yyyy.MM.dd HH:mm:ss",

      "yyyy-MM-dd HH:mm:ss",
      "yyyy-MM-dd hh:mm:ss",
      "yyyy-MM-dd HH:mm",
      "yyyy-MM-dd hh:mm",
      "yyyy-MM-dd'T'HH:mm:ss'Z'",

      "yyyy年MM月dd日",
      "yyyy年MM月dd日 HH:mm",
      "yyyy年MM月dd日 hh:mm",
      "yyyy年MM月dd日 HH:mm:ss",
      "yyyy年MM月dd日 hh:mm:ss",

      "yyyy/MM/dd",
      "yyyy/MM/dd HH:mm",
      "yyyy/MM/dd hh:mm",
      "yyyy/MM/dd HH:mm:ss",
      "yyyy/MM/dd hh:mm:ss",
      "yyyy/MM/dd HH:mm:ss.SSS zzz",
      "yyyy/MM/dd HH:mm:ss.SSS",
      "yyyy/MM/dd HH:mm:ss zzz",

      "MMM dd yyyy HH:mm:ss. zzz",
      "MMM dd yyyy HH:mm:ss zzz",
      "dd.MM.yyyy HH:mm:ss zzz",
      "dd MM yyyy HH:mm:ss zzz",
      "dd.MM.yyyy zzz",
      "dd.MM.yyyy; HH:mm:ss",
      "dd.MM.yyyy HH:mm:ss",

      "EEE MMM dd HH:mm:ss yyyy",
      "EEE MMM dd HH:mm:ss yyyy zzz",
      "EEE MMM dd HH:mm:ss zzz yyyy",
      "EEE, dd MMM yyyy HH:mm:ss zzz",
      "EEE,dd MMM yyyy HH:mm:ss zzz",
      "EEE, dd MMM yyyy HH:mm:sszzz",
      "EEE, dd MMM yyyy HH:mm:ss",
      "EEE, dd-MMM-yy HH:mm:ss zzz"
  };

  public static SimpleDateFormat DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  public static SimpleDateFormat FilesystemSafeDateFormat = new SimpleDateFormat("MMdd.hhmmss");

  public static long[] TIME_FACTOR = { 60 * 60 * 1000, 60 * 1000, 1000 };

  public static String format(long time) {
    return DateFormat.format(time);
  }

  public static String format(long time, String format) {
    return new SimpleDateFormat(format).format(time);
  }

  public static String now() {
    return DateFormat.format(System.currentTimeMillis());
  }

  public static String solrCompatibleFormat(long time) {
    return DateTimeFormatter.ISO_INSTANT.format(new Date(time).toInstant());
  }

  public static String solrCompatibleFormat(Date date) {
    return DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
  }

  public static String now(String format) {
    return format(System.currentTimeMillis(), format);
  }

  public static String elapsedTime(long start) {
    return elapsedTime(start, System.currentTimeMillis());
  }

  public static double elapsedSeconds(long start) {
    return (System.currentTimeMillis() - start) / 1000.0;
  }

  /**
   * Calculate the elapsed time between two times specified in milliseconds.
   *
   * @param start
   *          The start of the time period
   * @param end
   *          The end of the time period
   * @return a string of the form "XhYmZs" when the elapsed time is X hours, Y
   *         minutes and Z seconds or null if start > end.
   */
  public static String elapsedTime(long start, long end) {
    if (start > end) {
      return null;
    }

    long[] elapsedTime = new long[TIME_FACTOR.length];

    for (int i = 0; i < TIME_FACTOR.length; i++) {
      elapsedTime[i] = start > end ? -1 : (end - start) / TIME_FACTOR[i];
      start += TIME_FACTOR[i] * elapsedTime[i];
    }

    NumberFormat nf = NumberFormat.getInstance();
    nf.setMinimumIntegerDigits(2);
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < elapsedTime.length; i++) {
      if (i > 0) {
        buf.append(":");
      }
      buf.append(nf.format(elapsedTime[i]));
    }

    return buf.toString();
  }

  public static Date tryExtractDate(String dateStr) {
    return null;
  }

  public static long parseTime(String date) {
    long time = -1;
    try {
      time = HttpDateFormat.toLong(date);
    } catch (ParseException e) {
      try {
        Date parsedDate = DateUtils.parseDate(date, GENERAL_DATE_TIME_FORMATS);
        time = parsedDate.getTime();
      } catch (Exception e2) {

      }
    }
    return time;
  }
}
