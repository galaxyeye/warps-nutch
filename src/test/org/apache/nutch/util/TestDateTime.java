package org.apache.nutch.util;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TimeZone;

import static org.apache.commons.lang3.time.DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT;

/**
 * Created by vincent on 16-7-20.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class TestDateTime {

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Test
  public void testDateTimeConvert() {
    // Parse string into local date. LocalDateTime has no timezone component
    LocalDateTime time = LocalDateTime.parse("2014-04-16T13:00:00");

    // Convert to Instant with no time zone offset
    Instant instant = time.atZone(ZoneOffset.ofHours(0)).toInstant();

    // Easy conversion from Instant to the java.sql.Timestamp object
    Timestamp timestamp = Timestamp.from(instant);

    // Convert to LocalDateTime. Use no offset for timezone
    time = LocalDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.ofHours(0));

    // Add time. In this case, add one day.
    time = time.plus(1, ChronoUnit.DAYS);

    // Convert back to instant, again, no time zone offset.
    Instant output = time.atZone(ZoneOffset.ofHours(0)).toInstant();

    Timestamp savedTimestamp = Timestamp.from(output);

    time = LocalDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.ofHours(0));
    String formatted = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(time);

    System.out.println(formatted);
  }

  @Test
  public void testDuration() {
    Instant epoch = Instant.EPOCH;
    Instant now = Instant.now();

    Duration gap = Duration.between(epoch, now);
    System.out.println(gap.toDays());
    System.out.println(gap);

    long days = ChronoUnit.DAYS.between(epoch, now);
    System.out.println(days);

    System.out.println(Duration.ofDays(365 * 100).getSeconds());

    System.out.println(Duration.ofMinutes(60).toMillis());

    System.out.println(DurationFormatUtils.formatDuration(gap.toMillis(), "d\' days \'H\' hours \'m\' minutes \'s\' seconds\'"));
    System.out.println(DurationFormatUtils.formatDuration(gap.toMillis(), "d\'days\' H:mm:ss"));
  }

  @Test
  public void testDateFormat() {
    String dateString = "Sat May 27 12:21:42 CST 2017";

    try {
      Date date = DateUtils.parseDate(dateString, DateTimeUtil.GENERAL_DATE_TIME_FORMATS);
      // Date date = DateUtils.parseDate(dateString);
      dateString = DateFormatUtils.format(date, ISO_DATETIME_TIME_ZONE_FORMAT.getPattern(), TimeZone.getTimeZone("PRC"));
      System.out.println(dateString);

      dateString = DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
      System.out.println(dateString);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    Date date = new Date();
    dateString = DateFormatUtils.format(date, ISO_DATETIME_TIME_ZONE_FORMAT.getPattern());
    System.out.println(dateString);

    dateString = DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
    System.out.println(dateString);

    Instant now = Instant.now();
    System.out.println(now);

    LocalDateTime ldt = LocalDateTime.now();
    System.out.println(ldt);
  }

  @Test
  public void testIlligalDateFormat() {
    String dateString = "2013-39-08 10:39:36";
    try {
      Date date = formatter.parse(dateString);
      dateString = DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
      System.out.println(dateString);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDecimalFormat() {
    DecimalFormat df = new DecimalFormat("0.0##");
    System.out.println(df.format(.5678));
    System.out.println(df.format(6.5));
    System.out.println(df.format(56.5678));
    System.out.println(df.format(123456.5678));

    System.out.println(df.format(.0));
    System.out.println(df.format(6.00));
    System.out.println(df.format(56.0000));
    System.out.println(df.format(123456.00001));
  }

  @Test
  public void testStringFormat() {
    System.out.println(String.format("%1$,20d", -3123));
    System.out.println(String.format("%1$9d", -31));
    System.out.println(String.format("%1$-9d", -31));
    System.out.println(String.format("%1$(9d", -31));
    System.out.println(String.format("%1$#9x", 5689));
  }

}
