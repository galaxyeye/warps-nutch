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
package org.apache.nutch.filter.urlfilter;

// JDK imports
import org.apache.nutch.filter.URLFilter;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

import static org.junit.Assert.*;

// Logging imports
// Nutch imports

//import org.junit.runners.Suite;
//import org.junit.runner.RunWith;

/**
 * JUnit based test of class <code>RegexURLFilterBase</code>.
 */

// @RunWith(Suite.class)
// @Suite.SuiteClasses({TestAutomatonURLFilter.class, TestRegexURLFilter.class})
public abstract class RegexURLFilterBaseTest {

  /** My logger */
  protected static final Logger LOG = LoggerFactory.getLogger(RegexURLFilterBaseTest.class);

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  protected abstract URLFilter getURLFilter(Reader rules);

  protected void bench(int loops, String file) {
    try {
      bench(loops, new FileReader(SAMPLES + SEPARATOR + file + ".rules"),
          new FileReader(SAMPLES + SEPARATOR + file + ".urls"));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  protected void bench(int loops, Reader rules, Reader urls) {
    long start = System.currentTimeMillis();
    try {
      URLFilter filter = getURLFilter(rules);
      ArrayList<FilteredURL> expected = readURLFile(urls);
      for (int i = 0; i < loops; i++) {
        test(filter, expected);
      }
    } catch (Exception e) {
      fail(e.toString());
    }
    LOG.info("bench time (" + loops + ") "
        + (System.currentTimeMillis() - start) + "ms");
  }

  protected void test(String file) {
    try {
      LOG.info("Rules File : " + SAMPLES + SEPARATOR + file + ".rules");
      LOG.info("Urls File : " + SAMPLES + SEPARATOR + file + ".urls");

      test(new FileReader(SAMPLES + SEPARATOR + file + ".rules"),
          new FileReader(SAMPLES + SEPARATOR + file + ".urls"));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  protected void test(Reader rules, Reader urls) {
    try {
      test(getURLFilter(rules), readURLFile(urls));
    } catch (Exception e) {
      fail(StringUtil.stringifyException(e));
    }
  }

  protected void test(URLFilter filter, ArrayList<FilteredURL> expected) {
    expected.forEach(url -> {
      String result = filter.filter(url.url);
      if (result != null) {
        assertTrue(url.url, url.sign);
      } else {
        assertFalse(url.url, url.sign);
      }
    });
  }

  private static ArrayList<FilteredURL> readURLFile(Reader reader) throws IOException {
    return new BufferedReader(reader).lines().map(FilteredURL::new).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
  }

  private static class FilteredURL {
    boolean sign;
    String url;

    FilteredURL(String line) {
      switch (line.charAt(0)) {
      case '+':
        sign = true;
        break;
      case '-':
        sign = false;
        break;
      default:
        // Simply ignore...
      }
      url = line.substring(1);
    }
  }
}
