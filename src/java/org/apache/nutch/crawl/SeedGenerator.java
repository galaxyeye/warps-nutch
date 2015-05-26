/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.crawl;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.nutch.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * TODO : more generic seed generator
 * */
public class SeedGenerator {

  public static final Logger LOG = LoggerFactory.getLogger(SeedGenerator.class);

  public static void main(String[] args) throws Exception {
    String urlFormat = "http://oumen.com/detail.php?atid={{{1000,4460}}}";
    String[] urlParts = urlFormat.split("\\{\\{\\{\\d+\\,\\d+\\}\\}\\}");
    String[] placeholders = StringUtils.substringsBetween(urlFormat, "{{{", "}}}");

    ArrayList<ArrayList<Integer>> ranges = Lists.newArrayList();
    for (int i = 0; i < placeholders.length; ++i) {
      int min = Integer.parseInt(StringUtils.substringBefore(placeholders[i], ","));
      int max = Integer.parseInt(StringUtils.substringAfter(placeholders[i], ","));

      ranges.add(Lists.newArrayList(min, max));
    }

    // we can support only one placeholder right now

    StringBuilder content = new StringBuilder();
    for (int i = ranges.get(0).get(0); i <= ranges.get(0).get(1); ++i) {
      String url = urlParts[0] + i;
      if (urlParts.length > 1) {
        url += urlParts[1];
      }

      content.append(url);
      content.append("\n");
    }

    String tidyDomain = NetUtil.getTopLevelDomain(urlFormat);
    String file = StringUtils.substringBefore(tidyDomain, ".").toLowerCase().replaceAll("[^a-z]", "_");

    file = "/tmp/" + file + ".txt";
    FileUtils.writeStringToFile(new File(file), content.toString(), "utf-8");

    System.out.println("url seed results are saved in : " + file);
  }
}
