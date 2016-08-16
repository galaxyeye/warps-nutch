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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.awt.event.KeyEvent;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A minimal String utility class. Designed for internal jsoup use only.
 */
public final class StringUtil {

  public static final Pattern PricePattern = Pattern.compile("[1-9](,{0,1}\\d+){0,8}(\\.\\d{1,2})|[1-9](,{0,1}\\d+){0,8}");

  // all special chars on a standard keyboard
  public static final String DefaultKeepChars = "~!@#$%^&*()_+`-={}|[]\\:\";'<>?,./' \n\r\t";

  public static final String KeyboardWhitespace = String.valueOf(' ');

  public static final String NBSP = String.valueOf(' ');

  public static final Pattern PatternTime = Pattern.compile("[0-2][0-3]:[0-5][0-9]");

  public static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
      'f' };

  // memoised padding up to 10
  public static final String[] padding = { "", " ", "  ", "   ", "    ", "     ", "      ", "       ", "        ",
      "         ", "          " };

  public static final Comparator<String> LongerFirstComparator = new Comparator<String>() {
    public int compare(String s, String s2) {
      int result = Integer.compare(s2.length(), s.length());
      if (result == 0)
        return s.compareTo(s2);
      return result;
    }
  };

  public static final Comparator<String> ShorterFirstComparator = new Comparator<String>() {
    public int compare(String s, String s2) {
      return LongerFirstComparator.compare(s2, s);
    }
  };

  /**
   * Join a collection of strings by a seperator
   * 
   * @param strings
   *          collection of string objects
   * @param sep
   *          string to place between strings
   * @return joined string
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static String join(Collection strings, String sep) {
    return join(strings.iterator(), sep);
  }

  /**
   * Join a collection of strings by a seperator
   * 
   * @param strings
   *          iterator of string objects
   * @param sep
   *          string to place between strings
   * @return joined string
   */
  public static String join(Iterator<String> strings, String sep) {
    if (!strings.hasNext())
      return "";

    String start = strings.next().toString();
    if (!strings.hasNext()) // only one, avoid builder
      return start;

    StringBuilder sb = new StringBuilder(64).append(start);
    while (strings.hasNext()) {
      sb.append(sep);
      sb.append(strings.next());
    }
    return sb.toString();
  }

  /**
   * Returns space padding
   * 
   * @param width
   *          amount of padding desired
   * @return string of spaces * width
   */
  public static String padding(int width) {
    if (width < 0)
      throw new IllegalArgumentException("width must be > 0");

    if (width < padding.length)
      return padding[width];

    char[] out = new char[width];
    for (int i = 0; i < width; i++)
      out[i] = ' ';
    return String.valueOf(out);
  }

  /**
   * Tests if a string is blank: null, emtpy, or only whitespace (" ", \r\n, \t,
   * etc)
   * 
   * @param string
   *          string to test
   * @return if string is blank
   */
  public static boolean isBlank(String string) {
    if (string == null || string.length() == 0)
      return true;

    int l = string.length();
    for (int i = 0; i < l; i++) {
      if (!StringUtil.isWhitespace(string.codePointAt(i)))
        return false;
    }
    return true;
  }

  /**
   * Tests if a string is numeric, i.e. contains only digit characters
   * 
   * @param string
   *          string to test
   * @return true if only digit chars, false if empty or null or contains
   *         non-digit chrs
   */
  public static boolean isNumeric(String string) {
    if (string == null || string.length() == 0)
      return false;

    int l = string.length();
    for (int i = 0; i < l; i++) {
      if (!Character.isDigit(string.codePointAt(i)))
        return false;
    }
    return true;
  }

  /**
   * Tests if a code point is "whitespace" as defined in the HTML spec.
   * 
   * @param c
   *          code point to test
   * @return true if code point is whitespace, false otherwise
   */
  public static boolean isWhitespace(int c) {
    // note : the first character is not the last one
    // the last one is &nbsp;
    // String s = " ";
    // String s2 = " ";
    // System.out.println(s.equals(s2));
    return c == KeyboardWhitespace.charAt(0) || c == '\t' || c == '\n' || c == '\f' || c == '\r' || c == NBSP.charAt(0);
  }

  /**
   * Convenience call for {@link #toHexString(ByteBuffer, String, int)}, where
   * <code>sep = null; lineLen = Integer.MAX_VALUE</code>.
   * 
   * @param buf
   */
  public static String toHexString(ByteBuffer buf) {
    return toHexString(buf, null, Integer.MAX_VALUE);
  }

  /**
   * Get a text representation of a ByteBuffer as hexadecimal String, where each
   * pair of hexadecimal digits corresponds to consecutive bytes in the array.
   * 
   * @param buf
   *          input data
   * @param sep
   *          separate every pair of hexadecimal digits with this separator, or
   *          null if no separation is needed.
   * @param lineLen
   *          break the output String into lines containing output for lineLen
   *          bytes.
   */
  public static String toHexString(ByteBuffer buf, String sep, int lineLen) {
    return toHexString(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining(), sep, lineLen);
  }

  /**
   * Convenience call for {@link #toHexString(byte[], String, int)}, where
   * <code>sep = null; lineLen = Integer.MAX_VALUE</code>.
   * 
   * @param buf
   */
  public static String toHexString(byte[] buf) {
    return toHexString(buf, null, Integer.MAX_VALUE);
  }

  /**
   * Get a text representation of a byte[] as hexadecimal String, where each
   * pair of hexadecimal digits corresponds to consecutive bytes in the array.
   * 
   * @param buf
   *          input data
   * @param sep
   *          separate every pair of hexadecimal digits with this separator, or
   *          null if no separation is needed.
   * @param lineLen
   *          break the output String into lines containing output for lineLen
   *          bytes.
   */
  public static String toHexString(byte[] buf, String sep, int lineLen) {
    return toHexString(buf, 0, buf.length, sep, lineLen);
  }

  /**
   * Get a text representation of a byte[] as hexadecimal String, where each
   * pair of hexadecimal digits corresponds to consecutive bytes in the array.
   * 
   * @param buf
   *          input data
   * @param of
   *          the offset into the byte[] to start reading
   * @param cb
   *          the number of bytes to read from the byte[]
   * @param sep
   *          separate every pair of hexadecimal digits with this separator, or
   *          null if no separation is needed.
   * @param lineLen
   *          break the output String into lines containing output for lineLen
   *          bytes.
   */
  public static String toHexString(byte[] buf, int of, int cb, String sep, int lineLen) {
    if (buf == null)
      return null;
    if (lineLen <= 0)
      lineLen = Integer.MAX_VALUE;
    StringBuffer res = new StringBuffer(cb * 2);
    for (int c = 0; c < cb; c++) {
      int b = buf[of++];
      res.append(HEX_DIGITS[(b >> 4) & 0xf]);
      res.append(HEX_DIGITS[b & 0xf]);
      if (c > 0 && (c % lineLen) == 0)
        res.append('\n');
      else if (sep != null && c < lineLen - 1)
        res.append(sep);
    }
    return res.toString();
  }

  /**
   * Convert a String containing consecutive (no inside whitespace) hexadecimal
   * digits into a corresponding byte array. If the number of digits is not
   * even, a '0' will be appended in the front of the String prior to
   * conversion. Leading and trailing whitespace is ignored.
   * 
   * @param text
   *          input text
   * @return converted byte array, or null if unable to convert
   */
  public static byte[] fromHexString(String text) {
    text = text.trim();
    if (text.length() % 2 != 0)
      text = "0" + text;
    int resLen = text.length() / 2;
    int loNibble, hiNibble;
    byte[] res = new byte[resLen];
    for (int i = 0; i < resLen; i++) {
      int j = i << 1;
      hiNibble = charToNibble(text.charAt(j));
      loNibble = charToNibble(text.charAt(j + 1));
      if (loNibble == -1 || hiNibble == -1)
        return null;
      res[i] = (byte) (hiNibble << 4 | loNibble);
    }
    return res;
  }

  public static String normaliseWhitespace(String string) {
    string = string.replaceAll(NBSP, KeyboardWhitespace);
    string = string.replaceAll("&nbsp;", KeyboardWhitespace);

    StringBuilder sb = new StringBuilder(string.length());

    boolean lastWasWhite = false;
    boolean modified = false;

    int l = string.length();
    int c;
    for (int i = 0; i < l; i += Character.charCount(c)) {
      c = string.codePointAt(i);
      if (isWhitespace(c)) {
        if (lastWasWhite) {
          modified = true;
          continue;
        }

        if (c != KeyboardWhitespace.charAt(0))
          modified = true;
        sb.append(' ');
        lastWasWhite = true;
      } else {
        sb.appendCodePoint(c);
        lastWasWhite = false;
      }
    }

    return modified ? sb.toString() : string;
  }

  public static boolean in(String needle, String... haystack) {
    for (String hay : haystack) {
      if (hay.equals(needle))
        return true;
    }

    return false;
  }

  public static String trim(String s, int width) {
    if (s.length() > width)
      return s.substring(0, width - 1) + ".";
    else
      return s;
  }

  /**
   * Checks if a string is empty (ie is null or empty).
   */
  public static boolean isEmpty(String str) {
    return (str == null) || (str.equals(""));
  }

  // 根据Unicode编码完美的判断中文汉字和符号
  public static boolean isChinese(char c) {
    Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
    if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
        || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
        || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
        || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
        || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
        || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
        || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
      return true;
    }

    return false;
  }

  // 完整的判断中文汉字和符号
  public static boolean isChinese(String text) {
    char[] ch = text.toCharArray();
    for (int i = 0; i < ch.length; i++) {
      char c = ch[i];
      if (isChinese(c)) {
        return true;
      }
    }

    return false;
  }

  public static boolean isMainlyChinese(String text, double percentage) {
    if (text == null || text == "")
      return false;

    return 1.0 * countChinese(text) / text.length() >= percentage;
  }

  public static int countChinese(String text) {
    if (text == null || text == "")
      return 0;

    int count = 0;
    char[] ch = text.toCharArray();
    for (int i = 0; i < ch.length; i++) {
      if (isChinese(ch[i])) {
        ++count;
      }
    }

    return count;
  }

  public static boolean isCJK(char ch) {
    return Character.UnicodeBlock.of(ch) == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS;
  }

  // 对整个字符串：
  // 1. 仅保留英文字符、数字、汉字字符和keeps中的字符
  // 2. 去除网页空白：&nbsp;
  //
  // String attrName = "配 送 至：京 东 价：当&nbsp;当&nbsp;价";
  // attrName = StringUtils.strip(attrName).replaceAll("[\\s+:：(&nbsp;)]", "");
  // the "blank" characters in the above phrase can not be stripped
  public static String stripNonCJKChar(String text) {
    return stripNonCJKChar(text, null);
  }

  public static String stripNonCJKChar(String text, String keeps) {
    StringBuilder builder = new StringBuilder();

    if (keeps == null) {
      keeps = "";
    }

    for (int i = 0; i < text.length(); ++i) {
      char ch = text.charAt(i);
      if (Character.isLetterOrDigit(ch) || isCJK(ch)) {
        builder.append(ch);
      } else if (!keeps.equals("") && keeps.indexOf(ch) != -1) {
        builder.append(ch);
      }
    }

    return builder.toString();
  }

  public static String trimNonCJKChar(String text) {
    return trimNonCJKChar(text, null);
  }

  // 对字符串的头部和尾部：
  // 1. 仅保留英文字符、数字、汉字字符和keeps中的字符
  // 2. 去除网页空白：&nbsp;
  public static String trimNonCJKChar(String text, String keeps) {
    int start = 0;
    int end = text.length();
    if (keeps == null)
      keeps = "";

    for (int i = 0; i < text.length(); ++i) {
      char ch = text.charAt(i);
      if (Character.isLetterOrDigit(ch) || isCJK(ch) || keeps.indexOf(ch) != -1) {
        start = i;
        break;
      }
    }

    for (int i = text.length() - 1; i >= 0; --i) {
      char ch = text.charAt(i);
      if (Character.isLetterOrDigit(ch) || isCJK(ch) || keeps.indexOf(ch) != -1) {
        end = i + 1;
        break;
      }
    }

    return text.substring(start, end);
  }

  /**
   * Copy from org.apache.nutch.indexwriter.solr.SolrUtils
   * */
  public static String stripNonCharCodepoints(String input) {
    StringBuilder retval = new StringBuilder();
    char ch;

    for (int i = 0; i < input.length(); i++) {
      ch = input.charAt(i);

      // Strip all non-characters
      // http://unicode.org/cldr/utility/list-unicodeset.jsp?a=[:Noncharacter_Code_Point=True:]
      // and non-printable control characters except tabulator, new line and
      // carriage return
      if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {

        retval.append(ch);
      }
    }

    return retval.toString();
  }

  /**
   * TODO : not carefully implemented, and compare with #stripNonCharCodepoints
   * */
  public static String stripNonPrintableChar(String text) {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < text.length(); ++i) {
      char ch = text.charAt(i);
      if (isPrintableUnicodeChar(ch) && !Character.isWhitespace(ch)) {
        builder.append(ch);
      }
    }

    return builder.toString();
  }

  public static String stripTableTags(String text) {
    String regex = "table|tbody|tr|th|td";
    text = text.replaceAll(regex, "div");
    return text;
  }

  /**
   * TODO : not carefully implemented
   * */
   public static boolean isPrintableUnicodeChar(char ch) {
    Character.UnicodeBlock block = Character.UnicodeBlock.of(ch);
    return (!Character.isISOControl(ch)) && ch != KeyEvent.CHAR_UNDEFINED && block != null
        && block != Character.UnicodeBlock.SPECIALS;
  }

  /**
   * Copy from nutch , remove "�"
   *
   * @param value the dirty String value.
   * @return clean String
   */
  public static String cleanField(String value) {
    value = value.replaceAll("�", "");
    return value;
  }

  /**
   * Copy from nutch
   *
   * Convert utf8 char sequence to a string, and remove "�"
   *
   * @param utf8
   *          Utf8 object
   * @return string-ifed Utf8 object or null if Utf8 instance is null
   */
  public static String toString(CharSequence utf8) {
    return (utf8 == null ? null : cleanField(utf8.toString()));
  }

  public static String getLongestPart(final String text, final Pattern pattern) {
    String[] parts = pattern.split(text);

    if (parts.length == 1) {
      return "";
    }

    String longestPart = "";
    for (int i = 0; i < parts.length; i++) {
      String p = parts[i];

      if (p.length() > longestPart.length()) {
        longestPart = p;
      }
    }

    if (longestPart.length() == 0) {
      return "";
    } else {
      return longestPart.trim();
    }
  }

  public static int countTimeString(String text) {
    Matcher matcher = PatternTime.matcher(text);
    int count = 0;
    while (matcher.find())
      count++;
    return count;
  }

  public static String getLongestPart(final String text, final String regex) {
    return getLongestPart(text, Pattern.compile(regex));
  }

  public static String csslize(String text) {
    text = StringUtils.uncapitalize(text).trim();
    text = StringUtils.join(text.split("(?=\\p{Upper})"), "-").toLowerCase();
    text = text.replaceAll("[-_]", " ");
    text = text.replaceAll("\\s+", " ");

    return text;
  }

  // nav_top -> nav top, mainMenu -> main menu, image-detail -> image detail
  public static String humanize(String text) {
    text = StringUtils.join(text.split("(?=\\p{Upper})"), " ");
    text = text.replaceAll("[-_]", " ").toLowerCase().trim();

    return text;
  }

  public static double tryParseDouble(String s) {
    return tryParseDouble(s, 0.0);
  }

  public static double tryParseDouble(String s, double defaultValue) {
    double r = defaultValue;

    if (StringUtils.isEmpty(s)) {
      return r;
    }

    try {
      r = Double.parseDouble(s);
    } catch (Exception e) {
    }

    return r;
  }

  public static int tryParseInt(String s) {
    return tryParseInt(s, 0);
  }

  public static int tryParseInt(String s, int defaultValue) {
    int r = defaultValue;

    if (StringUtils.isEmpty(s)) {
      return r;
    }

    try {
      r = Integer.parseInt(s);
    } catch (Exception e) {
    }

    return r;
  }

  public static boolean contains(String text, CharSequence... searchCharSequences) {
    Validate.notNull(searchCharSequences);

    for (CharSequence search : searchCharSequences) {
      if (!text.contains(search)) return false;
    }

    return true;
  }

  public static boolean containsAny(String text, CharSequence... searchCharSequences) {
    Validate.notNull(searchCharSequences);

    for (CharSequence search : searchCharSequences) {
      if (text.contains(search)) return true;
    }

    return false;
  }

  public static boolean containsNone(String text, CharSequence... searchCharSequences) {
    Validate.notNull(searchCharSequences);

    for (CharSequence search : searchCharSequences) {
      if (text.contains(search)) return false;
    }

    return true;
  }

  public static String stringifyException(Throwable e) {
    return org.apache.hadoop.util.StringUtils.stringifyException(e);
  }

  private static int charToNibble(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      return -1;
    }
  }


  /**
   * Convert K/V pairs array into a map.
   *
   * @param params A K/V pairs array, the length of the array must be a even number
   *                null key or null value pair is ignored
   * @return A map contains all non-null key/values
   * */
  public static final Map<String, Object> toArgMap(Object... params) {
    HashMap<String, Object> results = new LinkedHashMap<>();

    if (params == null || params.length < 2) {
      return results;
    }

    if (params.length % 2 != 0) {
      throw new RuntimeException("expected name/value pairs");
    }

    for (int i = 0; i < params.length; i += 2) {
      if (params[i] != null && params[i + 1] != null) {
        results.put(String.valueOf(params[i]), params[i + 1]);
      }
    }

    return results;
  }

  public static String formatParams(Object... args) {
    return formatParamsMap(toArgMap(args));
  }

  public static String formatParamsMap(Map<String, Object> params) {
    if (params.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();

    sb.append('\n');
    sb.append(String.format("%10sParams Table%-15s\n", "----------", "----------"));
    sb.append(String.format("%15s   %-15s\n", "Name", "Value"));
    int i = 0;
    for (Map.Entry<String, Object> arg : params.entrySet()) {
      if (i++ > 0) {
        sb.append("\n");
      }

      sb.append(String.format("%15s", arg.getKey()));
      sb.append(" : ");
      sb.append(arg.getValue());
    }

    sb.append('\n');

    return sb.toString();
  }
}
