package org.apache.nutch.util;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

import static org.apache.commons.lang3.time.DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT;

/**
 * Created by vincent on 16-7-20.
 */
public class TestFormat {

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Test
  public void testDateFormat() {
    String dateString = "Sat May 27 12:21:42 CST 2017";

    try {
      Date date = DateUtils.parseDate(dateString, TimingUtil.GENERAL_DATE_TIME_FORMATS);
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
