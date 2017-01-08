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

package org.apache.nutch.parse.tika;

import com.google.common.collect.Lists;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.MimeUtil;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * Unit tests for the RSS Parser based on John Xing's TestPdfParser class.
 * 
 * @author mattmann
 * @version 1.0
 */
public class TestRSSParser {

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-rss/build.xml during plugin compilation.

  private String[] sampleFiles = { "rsstest.rss" };

  /**
   * <p>
   * The test method: tests out the following 2 asserts:
   * </p>
   * 
   * <ul>
   * <li>There are 3 outlinks read from the sample rss file</li>
   * <li>The 3 outlinks read are in fact the correct outlinks from the sample
   * file</li>
   * </ul>
   */
  @Test
  public void testIt() throws ProtocolException, ParseException, IOException {
    String urlString;
    ParseResult parseResult;

    Configuration conf = ConfigUtils.create();
    MimeUtil mimeutil = new MimeUtil(conf);
    for (String sampleFile : sampleFiles) {
      urlString = "file:" + sampleDir + fileSeparator + sampleFile;

      File file = new File(sampleDir + fileSeparator + sampleFile);
      byte[] bytes = new byte[(int) file.length()];
      DataInputStream in = new DataInputStream(new FileInputStream(file));
      in.readFully(bytes);
      in.close();

      WebPage page = WebPage.newWebPage();
      page.setBaseUrl(new Utf8(urlString));
      page.setContent(bytes);
      String mtype = mimeutil.getMimeType(file);
      page.setContentType(mtype);

      parseResult = new ParseUtil(conf).parse(urlString, page);

      // check that there are 2 outlinks:
      // http://www-scf.usc.edu/~mattmann/
      // http://www.nutch.org
      List<String> urls = parseResult.getOutlinks().stream().map(Outlink::getToUrl).collect(Collectors.toList());
      if (!urls.containsAll(Lists.newArrayList("http://www-scf.usc.edu/~mattmann/", "http://www.nutch.org/"))) {
        fail("Outlinks read from sample rss file are not correct!");
      }
    }
  }
}
