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

package org.apache.nutch.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.ConfigUtils;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * TestCase to check regExp extraction of URLs.
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 * 
 * @version 1.0
 */
public class TestOutlinkExtractor {

  private static Configuration conf = ConfigUtils.create();

  @Test
  public void testGetNoOutlinks() {
    ArrayList<Outlink> outlinks = OutlinkExtractor.getOutlinks(null, conf);
    assertNotNull(outlinks);
    assertEquals(0, outlinks.size());

    outlinks = OutlinkExtractor.getOutlinks("", conf);
    assertNotNull(outlinks);
    assertEquals(0, outlinks.size());
  }

  @Test
  public void testGetOutlinksHttp() {
    ArrayList<Outlink> outlinks = OutlinkExtractor.getOutlinks(
            "Test with http://www.nutch.org/index.html is it found? "
                + "What about www.google.com at http://www.google.de "
                + "A longer URL could be http://www.sybit.com/solutions/portals.html",
            conf);

    assertTrue("Url not found!", outlinks.size() == 3);
    assertEquals("Wrong URL", "http://www.nutch.org/index.html", outlinks.get(0).getToUrl());
    assertEquals("Wrong URL", "http://www.google.de", outlinks.get(1).getToUrl());
    assertEquals("Wrong URL", "http://www.sybit.com/solutions/portals.html", outlinks.get(2).getToUrl());
  }

  @Test
  public void testGetOutlinksHttp2() {
    ArrayList<Outlink> outlinks = OutlinkExtractor.getOutlinks(
            "Test with http://www.nutch.org/index.html is it found? "
                + "What about www.google.com at http://www.google.de "
                + "A longer URL could be http://www.sybit.com/solutions/portals.html",
            "http://www.sybit.de", conf);

    assertTrue("Url not found!", outlinks.size() == 3);
    assertEquals("Wrong URL", "http://www.nutch.org/index.html", outlinks.get(0).getToUrl());
    assertEquals("Wrong URL", "http://www.google.de", outlinks.get(1).getToUrl());
    assertEquals("Wrong URL", "http://www.sybit.com/solutions/portals.html", outlinks.get(2).getToUrl());
  }

  @Test
  public void testGetOutlinksFtp() {
    ArrayList<Outlink> outlinks = OutlinkExtractor.getOutlinks(
        "Test with ftp://www.nutch.org is it found? "
            + "What about www.google.com at ftp://www.google.de", conf);

    assertTrue("Url not found!", outlinks.size() > 1);
    assertEquals("Wrong URL", "ftp://www.nutch.org", outlinks.get(0).getToUrl());
    assertEquals("Wrong URL", "ftp://www.google.de", outlinks.get(1).getToUrl());
  }
}
