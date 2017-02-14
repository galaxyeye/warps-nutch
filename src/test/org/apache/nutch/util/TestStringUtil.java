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
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for StringUtil methods. */
public class TestStringUtil {

  @Test
  public void testValueOf() {
    int i = 18760506;
    assertEquals("18760506", String.valueOf(i));
  }

  @Test
  public void testSubstring() {
    String s = "a,b,c,d";
    s = StringUtils.substringAfter(s, ",");
    assertEquals("b,c,d", s);

    s = "a\nb\nc\nd\ne\nf\ng\n";
    // assume the avarage lenght of a link is 100 characters
    int pos = StringUtils.indexOf(s, '\n', s.length() / 2);
    s = s.substring(pos + 1);
    assertEquals("e\nf\ng\n", s);
  }

  @Test
  public void testToHexString() {
    ByteBuffer buffer = ByteBuffer.wrap("".getBytes());
    assertEquals("", StringUtil.toHexString(buffer));
  }

  @Test
  public void testRegex() {
    String text = "http://aitxt.com/book/12313413874";
    String regex = "http://(.*)aitxt.com(.*)";
    assertTrue(text.matches(regex));

    text = "http://aitxt.com/book/12313413874";
    regex = ".*";
    assertTrue(text.matches(regex));
  }

  @Test
  public void testDecimalFormat() {
    DecimalFormat df = new DecimalFormat("0.0##");

    assertEquals("0.568", df.format(.5678));
    assertEquals("6.5", df.format(6.5));
    assertEquals("56.568", df.format(56.5678));
    assertEquals("123456.568", df.format(123456.5678));
    assertEquals("0.0", df.format(.0));
    assertEquals("6.0", df.format(6.00));
    assertEquals("56.0", df.format(56.0000));
    assertEquals("123456.0", df.format(123456.00001));
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
