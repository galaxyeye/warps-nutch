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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for StringUtil methods. */
public class TestStringUtil {

  @Test
  public void testValueOf() {
    int i = 18760506;
    System.out.println(String.valueOf(i));
  }

  @Test
  public void testSubstring() {
    String s = "a,b,c,d";
    StringUtils.substringAfter(s, ",");
    System.out.println(s);
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
}
