/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parse.html;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.util.ConfigUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHtmlParser {

    public static final Logger LOG = LoggerFactory.getLogger(TestHtmlParser.class);

    private static final String encodingTestKeywords = "français, español, русский язык, čeština, ελληνικά";
    private static final String encodingTestBody = "<ul>\n  <li>français\n  <li>español\n  <li>русский язык\n  <li>čeština\n  <li>ελληνικά\n</ul>";
    private static final String encodingTestContent = "<title>"
            + encodingTestKeywords + "</title>\n"
            + "<meta name=\"keywords\" content=\"" + encodingTestKeywords + "\"/>\n"
            + "</head>\n<body>" + encodingTestBody + "</body>\n</html>";

    private static String[][] encodingTestPages = {
            {
                    "HTML4, utf-8, meta http-equiv, no quotes",
                    "utf-8",
                    "success",
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" "
                            + "\"http://www.w3.org/TR/html4/loose.dtd\">\n"
                            + "<html>\n<head>\n"
                            + "<meta http-equiv=Content-Type content=\"text/html; charset=utf-8\" />"
                            + encodingTestContent
            },
            {
                    "HTML4, utf-8, meta http-equiv, single quotes",
                    "utf-8",
                    "success",
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" "
                            + "\"http://www.w3.org/TR/html4/loose.dtd\">\n"
                            + "<html>\n<head>\n"
                            + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8' />"
                            + encodingTestContent
            },
            {
                    "XHTML, utf-8, meta http-equiv, double quotes",
                    "utf-8",
                    "success",
                    "<?xml version=\"1.0\"?>\n<html xmlns=\"http://www.w3.org/1999/xhtml\">"
                            + "<html>\n<head>\n"
                            + "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                            + encodingTestContent
            },
            {
                    "HTML5, utf-8, meta charset",
                    "utf-8",
                    "success",
                    "<!DOCTYPE html>\n<html>\n<head>\n" + "<meta charset=\"utf-8\">"
                            + encodingTestContent
            },
            {
                    "HTML5, utf-8, BOM",
                    "utf-8",
                    "success",
                    "\ufeff<!DOCTYPE html>\n<html>\n<head>\n" + encodingTestContent
            },
            {
                    "HTML5, utf-16, BOM",
                    "utf-16",
                    "failed",
                    "\ufeff<!DOCTYPE html>\n<html>\n<head>\n" + encodingTestContent
            }
    };

    private Configuration conf;

    private static final String dummyUrl = "http://dummy.url/";

    @Before
    public void setup() {
        conf = ConfigUtils.create();
    }

    protected WebPage getPage(String html, Charset charset) throws ParseException {
        WebPage page = WebPage.newWebPage();
        page.setBaseUrl(dummyUrl);
        page.setContent(html.getBytes(charset));
        page.setContentType("text/html");

        new ParseUtil(conf).parse(dummyUrl, page);

        return page;
    }

    @Test
    public void testEncodingDetection() throws ParseException {
        for (String[] testPage : encodingTestPages) {
            String name = testPage[0];
            Charset charset = Charset.forName(testPage[1]);
            boolean success = testPage[2].equals("success");
            WebPage page = getPage(testPage[3], charset);

            String text = page.getPageText();
            String title = page.getPageTitle();
            String keywords = page.getMetadata(Metadata.Name.META_KEYWORDS);

            LOG.info("name : " + name + "\tcharset : " + charset + "\ttitle: " + title);
            LOG.info("keywords:>>>" + keywords + "<<<");
            LOG.info("text:>>>" + text + "<<<");

            if (success) {
                assertEquals("Title not extracted properly (" + name + ")", encodingTestKeywords, title);
                for (String keyword : encodingTestKeywords.split(",\\s*")) {
                    assertTrue(keyword + " not found in text (" + name + ")", text.contains(keyword));
                }
                assertEquals("Keywords not extracted properly (" + name + ")", encodingTestKeywords, keywords);
            }
        }
    }
}
