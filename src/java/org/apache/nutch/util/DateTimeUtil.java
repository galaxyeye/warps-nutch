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
import org.apache.commons.lang3.StringUtils;

import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DateTimeUtil {

  public static final int CURRENT_YEAR = Year.now().getValue();
  public static final String CURRENT_YEAR_STR = String.valueOf(CURRENT_YEAR);
  public static final int CURRENT_MONTH = YearMonth.now().getMonthValue();
  public static final int YEAR_LOWER_BOUND = 1990;

  public static Set<String> OLD_YEARS;
  public static Set<String> OLD_MONTH;
  public static Pattern OLD_MONTH_URL_DATE_PATTERN;

  static {
    OLD_YEARS = IntStream.range(YEAR_LOWER_BOUND, CURRENT_YEAR).mapToObj(String::valueOf).collect(Collectors.toSet());
    OLD_MONTH = IntStream.range(1, CURRENT_MONTH).mapToObj(m -> String.format("%02d", m)).collect(Collectors.toSet());
    // eg : ".+2016[/\.-]?(01|02|03|04|05|06|07|08|09).+"
    OLD_MONTH_URL_DATE_PATTERN = Pattern.compile(".+" + CURRENT_YEAR_STR + "[/\\.-]?(" + StringUtils.join(OLD_MONTH, "|") + ").+");
  }

  // 2016-03-05 20:07:51
  // TODO : What's the difference between HH and hh? Guess : 24 hours VS 12 hours
  public static String[] POSSIBLE_DATE_TIME_FORMATS = new String[]{
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

  public static SimpleDateFormat FilesystemSafeDateFormat = new SimpleDateFormat("MMdd.HHmmss");

  public static long[] TIME_FACTOR = {60 * 60 * 1000, 60 * 1000, 1000};

  public static String format(long time) {
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(time));
  }

  public static String format(Instant time) {
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(time);
  }

  public static String format(Instant time, String format) {
    return DateTimeFormatter.ofPattern(format).withZone(ZoneId.systemDefault()).format(time);
  }

  public static String format(LocalDateTime localTime) {
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(localTime);
  }

  public static String format(LocalDateTime localTime, String format) {
    return DateTimeFormatter.ofPattern(format).format(localTime);
  }

  public static String format(long epochMilli, String format) {
    return format(Instant.ofEpochMilli(epochMilli), format);
  }

  public static String now() {
    return format(LocalDateTime.now());
  }

  public static String isoInstantFormat(long time) {
    return DateTimeFormatter.ISO_INSTANT.format(new Date(time).toInstant());
  }

  public static String isoInstantFormat(Date date) {
    return DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
  }

  public static String isoInstantFormat(Instant time) {
    return DateTimeFormatter.ISO_INSTANT.format(time);
  }

  public static String now(String format) {
    return format(System.currentTimeMillis(), format);
  }

  public static String elapsedTime(long start) {
    return elapsedTime(start, System.currentTimeMillis());
  }

  public static String elapsedTime(Instant start) {
    return elapsedTime(start.toEpochMilli(), System.currentTimeMillis());
  }

  public static double elapsedSeconds(long start) {
    return (System.currentTimeMillis() - start) / 1000.0;
  }

  public static Instant parseHttpDateTime(String text, Instant defaultValue) {
    try {
      // RFC 2616 defines three different date formats that a conforming client must understand.
      Date d = org.apache.http.client.utils.DateUtils.parseDate(text);
      return d.toInstant();
    } catch (Throwable e) {
      return defaultValue;
    }
  }

  public static String formatHttpDateTime(Instant time) {
    return org.apache.http.client.utils.DateUtils.formatDate(Date.from(time));
  }

  public static Instant parseInstant(String text, Instant defaultValue) {
    try {
      // equals to Instant.parse()
      return DateTimeFormatter.ISO_INSTANT.parse(text, Instant::from);
    } catch (Throwable e) {
      return defaultValue;
    }
  }

  public static Date tryParseDateTime(String dateStr) {
    Date parsedDateTime = null;

    try {
      parsedDateTime = DateUtils.parseDate(dateStr, POSSIBLE_DATE_TIME_FORMATS);
    } catch (ParseException ignored) {
    }

    return parsedDateTime;
  }

  public static String constructTimeHistory(String timeHistory, Instant fetchTime, int maxRecords) {
    String dateStr = isoInstantFormat(fetchTime);

    if (timeHistory == null) {
      timeHistory = dateStr;
    }
    else {
      String[] fetchTimes = timeHistory.split(",");
      if (fetchTimes.length > maxRecords) {
        String firstFetchTime = fetchTimes[0];
        int start = fetchTimes.length - maxRecords;
        int end = fetchTimes.length;
        timeHistory = firstFetchTime + ',' + StringUtils.join(fetchTimes, ',', start, end);
      }
      timeHistory += ",";
      timeHistory += dateStr;
    }

    return timeHistory;
  }

  /**
   * For urls who contains date information, for example
   * http://bond.hexun.com/2011-01-07/126641872.html
   * */
  public static boolean containsOldDate(String str) {
    if (str == null) {
      return false;
    }

    if (OLD_YEARS.stream().anyMatch(str::contains)) {
      return true;
    }

    if (OLD_MONTH_URL_DATE_PATTERN.asPredicate().test(str)) {
      return true;
    }

    return false;
  }

  /**
   * Calculate the elapsed time between two times specified in milliseconds.
   *
   * @param start The start of the time period
   * @param end   The end of the time period
   * @return a string of the form "XhYmZs" when the elapsed time is X hours, Y
   * minutes and Z seconds or null if start > end.
   */
  private static String elapsedTime(long start, long end) {
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
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < elapsedTime.length; i++) {
      if (i > 0) {
        sb.append(":");
      }
      sb.append(nf.format(elapsedTime[i]));
    }

    return sb.toString();
  }
}
