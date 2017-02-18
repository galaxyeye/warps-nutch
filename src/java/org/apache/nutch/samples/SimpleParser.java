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

package org.apache.nutch.samples;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.common.EncodingDetector;
import org.apache.nutch.filter.CrawlFilters;
import org.apache.nutch.filter.URLFilterException;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warps.scent.document.TextDocument;
import org.warps.scent.sax.SAXInput;
import org.warps.scent.util.ProcessingException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parser checker, useful for testing parser. It also accurately reports
 * possible fetching and parsing failures and presents protocol status signals
 * to aid debugging. The tool enables us to retrieve the following data from any
 */
public class SimpleParser extends Configured {

  public static final Logger LOG = LoggerFactory.getLogger(SimpleParser.class);

  private CrawlFilters crawlFilters;
  private Map<String, Object> results = Maps.newLinkedHashMap();

  private WebPage page;
  private SimpleFetcher fetcher;
  private ParseResult parseResult;

  public SimpleParser(Configuration conf) {
    setConf(conf);
    crawlFilters = CrawlFilters.create(conf);
    fetcher = new SimpleFetcher(conf);
  }

  public WebPage parse(String url) { return parse(url, null); }

  public WebPage parse(String url, String contentType) {
    LOG.info("Parsing: " + url);

    page = fetcher.fetch(url, contentType);
    if (page != null) {
      parse(page);
    }

    return page;
  }

  public WebPage parse(WebPage page) {
    ParseUtil parseUtil = new ParseUtil(getConf());
    if (page != null && !page.getBaseUrl().isEmpty()) {
      parseResult = parseUtil.process(page.url(), page);
    }
    return page;
  }

  public WebPage extract(String url, String contentType) {
    LOG.info("Extract: " + url);

    try {
      page = fetcher.fetch(url, contentType);
      if (page != null) {
        extract(url, page);
      }
    } catch (SAXException|IOException|ProcessingException e) {
      LOG.error(e.getMessage());
    }

    return page;
  }

  private InputSource getContentAsInputSource(WebPage page) {
    ByteBuffer contentInOctets = page.getContent();

    ByteArrayInputStream stream = new ByteArrayInputStream(contentInOctets.array(),
        contentInOctets.arrayOffset() + contentInOctets.position(),
        contentInOctets.remaining());

    return new InputSource(stream);
  }

  public void extract(String url, WebPage page) throws IOException, SAXException, ProcessingException {
    InputSource input = getContentAsInputSource(page);

    EncodingDetector encodingDetector = new EncodingDetector(getConf());
    String encoding = encodingDetector.sniffEncoding(page);
    input.setEncoding(encoding);

    input = getContentAsInputSource(page);
    TextDocument scentDoc = new SAXInput().parse(url, input);
    System.out.println(scentDoc.getTextContent(true, true));
  }

  public WebPage getWebPage() {
    return page;
  }

  public Map<String, Object> getResult() {
    try {
      processResult();
    } catch (MalformedURLException|URLFilterException e) {
      LOG.error(e.getMessage());
    }

    return results;
  }

  private Map<String, Object> processResult() throws MalformedURLException, URLFilterException {
    if (parseResult == null) {
      LOG.error("Problem with parseResult - check log");
      return results;
    }

    String discardedOutlinks = parseResult.getOutlinks().stream()
        .map(Outlink::getToUrl)
        .filter(l -> !crawlFilters.normalizeAndValidateUrl(l)).sorted().distinct().collect(Collectors.joining("\n"));
    results.put("DiscardOutlinks", discardedOutlinks);

    return results;
  }

  private static class Options {
    @Parameter(required = true, description = "Url to parse")
    List<String> url = new ArrayList<>();
    @Parameter(names = {"-dumpText", "-t"}, description = "dumpText")
    boolean dumpText = false;
    @Parameter(names = {"-dumpContent", "-c"}, description = "dumpContent")
    boolean dumpContent = false;
    @Parameter(names = {"-dumpHeaders", "-h"}, description = "dumpHeaders")
    boolean dumpHeaders = false;
    @Parameter(names = {"-dumpLinks", "-l"}, description = "dumpLinks")
    boolean dumpLinks = false;
  }

  public static void main(String[] args) throws Exception {
    Options opt = new Options();
    JCommander jc = new JCommander(opt, args);

    if (args.length < 1) {
      jc.usage();
      System.exit(0);
    }

    SimpleParser parser = new SimpleParser(ConfigUtils.create());
    // parser.parseResult(url, contentType);
    WebPage page = parser.parse(opt.url.get(0));

    System.out.println(parser.getResult());
    System.out.println(page.getRepresentation(opt.dumpText, opt.dumpContent, opt.dumpHeaders, opt.dumpLinks));
  }
}
