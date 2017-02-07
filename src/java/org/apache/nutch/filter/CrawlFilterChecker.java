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

package org.apache.nutch.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.ConfigUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Checks one given filter or all filters.
 * 
 * @author John Xing
 */
public class CrawlFilterChecker {

  private Configuration conf;

  public CrawlFilterChecker(Configuration conf) {
    this.conf = conf;
  }

  private void checkAll() throws Exception {
    CrawlFilters filters = CrawlFilters.create(conf);

    System.out.println("CrawlFilter : " + filters.toString());

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = in.readLine()) != null) {
      String out = filters.normalizeUrlToEmpty(line);
      if (!out.isEmpty()) {
        System.out.print("+");
        System.out.println(out);
      } else {
        System.out.print("-");
        System.out.println(line);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    CrawlFilterChecker checker = new CrawlFilterChecker(ConfigUtils.create());
    checker.checkAll();

    System.exit(0);
  }
}
