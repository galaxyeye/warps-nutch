/*
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
package org.apache.nutch.filter.urlfilter.urlnormalizer.pass;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.filter.URLNormalizers;
import org.apache.nutch.filter.urlnormalizer.pass.PassURLNormalizer;
import org.apache.nutch.util.ConfigUtils;
import org.junit.Test;

import java.net.MalformedURLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestPassURLNormalizer {

  @Test
  public void testPassURLNormalizer() {
    Configuration conf = ConfigUtils.create();

    PassURLNormalizer normalizer = new PassURLNormalizer();
    normalizer.setConf(conf);
    String url = "http://www.example.com/test/..//";
    String result = null;
    try {
      result = normalizer.normalize(url, URLNormalizers.SCOPE_DEFAULT);
    } catch (MalformedURLException mue) {
      fail(mue.toString());
    }

    assertEquals(url, result);
  }
}
