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
package org.apache.nutch.filter.urlfilter.domain;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.ConfigUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestDomainURLFilter {

  protected static final Logger LOG = LoggerFactory.getLogger(TestDomainURLFilter.class);

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  private Configuration conf = ConfigUtils.create();

  @Test
  public void testGeneralFilter() throws Exception {
    String[] allowedUrls = {
        "http://sz.sxrb.com/sxxww/dspd/szpd/bwch/",
        "http://www.sxrb.com/sxxww/dspd/czpd/czsq/",
        "http://yc.sxrb.com/sxxww/dspd/ycpd/bwbd/",
        "http://yq.sxrb.com/sxxww/dspd/yqpd/fz/",
        "http://lucene.apache.org",
        "http://hadoop.apache.org",
        "http://www.apache.org",
        "http://www.foobar.net",
        "http://www.foobas.net",
        "http://www.yahoo.com",
        "http://www.foobar.be"
    };

    String[] disallowedUrls = {
        "http://www.google.com",
        "http://mail.yahoo.com",
        "http://www.adobe.com"
    };

    String domainFile = SAMPLES + SEPARATOR + "general" + SEPARATOR + "hosts.txt";
    DomainURLFilter domainFilter = new DomainURLFilter(domainFile, conf);

    Stream.of(allowedUrls).forEach(url -> assertNotNull(domainFilter.filter(url)));
    Stream.of(disallowedUrls).forEach(url -> assertNull(domainFilter.filter(url)));
  }

  @Test
  public void testInformationFilter() throws Exception {
    String[] allowedUrls = {
        "http://sz.sxrb.com/sxxww/dspd/szpd/bwch/",
        "http://www.sxrb.com/sxxww/dspd/czpd/czsq/",
        "http://yc.sxrb.com/sxxww/dspd/ycpd/bwbd/",
        "http://yq.sxrb.com/sxxww/dspd/yqpd/fz/",
    };

    String[] disallowedUrls = {
        "http://jz.sxrb.com/",
        "http://kqjk.sxrb.com/",
        "http://lf.sxrb.com/",
        "http://ll.sxrb.com/"
    };

    String domainFile = SAMPLES + SEPARATOR + "information" + SEPARATOR + "hosts.txt";
    DomainURLFilter domainFilter = new DomainURLFilter(domainFile, conf);

    Stream.of(allowedUrls).forEach(url -> assertNotNull(domainFilter.filter(url)));
    Stream.of(disallowedUrls).forEach(url -> assertNull(domainFilter.filter(url)));
  }
}
