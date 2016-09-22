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

import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Unit tests for StringUtil methods. */
public class TestStringUtil {

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
  public void testUrlPattern() {
    String[] urls = {
        "http://aitxt.com/2016/01/12313413874.html",
        "http://aitxt.com/2019/02/12283413826.html",
        "http://reli.cssn.cn/zjx/zjx_zjyj/zjx_jdjyj/201607/t20160722_3131878.shtml",
        "http://aitxt.com/book/12313413874",
        "http://aitxt.com/12313413874.html",
    };
    String regex = ".+(detail|item|article|book|good|product|thread|/20[012][0-9]/{0,1}[01][0-9]/|\\d{10,}).+";
    Pattern DETAIL_PAGE_URL_PATTERN = Pattern.compile(regex);

    for (String url : urls) {
      assertTrue(url.matches(regex));
      assertTrue(DETAIL_PAGE_URL_PATTERN.matcher(url).matches());
    }

    assertFalse("http://t.tt/".matches(regex));
  }
}
