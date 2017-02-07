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

import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.common.EncodingDetector;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.filter.CrawlFilters;
import org.apache.nutch.filter.URLFilterException;
import org.apache.nutch.jobs.parse.ParserMapper;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  private Map<String, Object> results = Maps.newHashMap();

  private String url;
  private WebPage page;
  private SimpleFetcher fetcher;
  private ParseResult parseResult;

  public SimpleParser(Configuration conf) {
    setConf(conf);
    crawlFilters = CrawlFilters.create(conf);
    fetcher = new SimpleFetcher(conf);
  }

  public void parse(String url) { parse(url, null); }

  public void parse(String url, String contentType) {
    LOG.info("Parsing: " + url);

    this.url = url;
    page = fetcher.fetch(url, contentType);
    if (page != null) {
      parse(page);
    }
  }

  public void parse(WebPage page) {
    ParseUtil parseUtil = new ParseUtil(getConf());
    if (page != null && !page.getBaseUrl().isEmpty()) {
      parseResult = parseUtil.process(url, page);
    }
  }

  public void extract(String url, String contentType) {
    LOG.info("Extract: " + url);

    try {
      this.url = url;
      page = fetcher.fetch(url, contentType);
      if (page != null) {
        extract(page);
      }
    } catch (SAXException|IOException|ProcessingException e) {
      LOG.error(e.getMessage());
    }
  }

  private InputSource getContentAsInputSource(WebPage page) {
    ByteBuffer contentInOctets = page.getContent();

    ByteArrayInputStream stream = new ByteArrayInputStream(contentInOctets.array(),
        contentInOctets.arrayOffset() + contentInOctets.position(),
        contentInOctets.remaining());

    return new InputSource(stream);
  }

  public void extract(WebPage page) throws IOException, SAXException, ProcessingException {
    InputSource input = getContentAsInputSource(page);

    EncodingDetector encodingDetector = new EncodingDetector(getConf());
    String encoding = encodingDetector.sniffEncoding(page);
    input.setEncoding(encoding);

    input = getContentAsInputSource(page);
    TextDocument scentDoc = new SAXInput(input).getTextDocument();
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

  private void saveWebPage(WebPage page) {
    try {
      Path path = Paths.get("/tmp/nutch/web/" + DigestUtils.md5Hex(page.getBaseUrl()));

      if (!Files.exists(path)) {
        FileUtils.forceMkdir(path.getParent().toFile());
        Files.createFile(path);
      }
      Files.write(path, page.getContent().array());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Map<String, Object> processResult() throws MalformedURLException, URLFilterException {
    if (parseResult == null) {
      LOG.error("Problem with parseResult - check log");
      return results;
    }

    results.put("content", page.getContent());

    if (ParserMapper.isTruncated(url, page)) {
      results.put("contentStatus", "truncated");
    }

    // Calculate the signature
    byte[] signature = SignatureFactory.getSignature(getConf()).calculate(page);
    results.put("Signature", StringUtil.toHexString(signature));

    String metadata = page.get().getMetadata().entrySet().stream()
        .map(e -> e.getKey() + " : " + e.getValue())
        .collect(Collectors.joining("\n"));
    results.put("Metadata", metadata);

    String validOutlinks = parseResult.getOutlinks().stream()
        .map(Outlink::getToUrl)
        .filter(l -> crawlFilters.normalizeAndValidateUrl(l)).sorted().distinct().collect(Collectors.joining("\n"));
    results.put("Outlinks", validOutlinks);

    String discardedOutlinks = parseResult.getOutlinks().stream()
        .map(Outlink::getToUrl)
        .filter(l -> !crawlFilters.normalizeAndValidateUrl(l)).sorted().distinct().collect(Collectors.joining("\n"));
    results.put("DiscardOutlinks", discardedOutlinks);

    String headerStr = page.getHeaders().entrySet().stream()
        .map(e -> e.getKey() + " : " + e.getValue())
        .collect(Collectors.joining("\n"));
    results.put("Headers", headerStr);

    return results;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage : SimpleParser [-forceAs contentType] url");
      System.exit(0);
    }

    String contentType = null;
    String url = null;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-forceAs")) {
        contentType = args[++i];
      } else {
        url = URLUtil.toASCII(args[i]);
      }
    }

    SimpleParser parser = new SimpleParser(ConfigUtils.create());
    // parser.parseResult(url, contentType);
    parser.parse(url, contentType);

    System.out.println(parser.getResult());
  }
}
