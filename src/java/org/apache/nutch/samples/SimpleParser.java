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
import com.google.common.collect.Sets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.fetch.FetchUtil;
import org.apache.nutch.mapreduce.ParserMapper;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.*;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.*;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Parser checker, useful for testing parser. It also accurately reports
 * possible fetching and parsing failures and presents protocol status signals
 * to aid debugging. The tool enables us to retrieve the following data from any
 * url:
 * <ol>
 * <li><tt>contentType</tt>: The URL {@link Content} type.</li>
 * <li><tt>signature</tt>: Digest is used to identify pages (like unique ID) and
 * is used to remove duplicates during the dedup procedure. It is calculated
 * using {@link org.apache.nutch.crawl.MD5Signature} or
 * {@link org.apache.nutch.crawl.TextProfileSignature}.</li>
 * <li><tt>Version</tt>: From {@link org.apache.nutch.parse}.</li>
 * <li><tt>Status</tt>: From {@link org.apache.nutch.parse}.</li>
 * <li><tt>Title</tt>: of the URL</li>
 * <li><tt>Outlinks</tt>: associated with the URL</li>
 * <li><tt>Content Metadata</tt>: such as <i>X-AspNet-Version</i>, <i>Date</i>,
 * <i>Content-length</i>, <i>servedBy</i>, <i>Content-Type</i>,
 * <i>Cache-Control</>, etc.</li>
 * <li><tt>Parse Metadata</tt>: such as <i>CharEncodingForConversion</i>,
 * <i>OriginalCharEncoding</i>, <i>language</i>, etc.</li>
 * <li><tt>ParseText</tt>: The page parse text which varies in length depdnecing
 * on <code>content.length</code> configuration.</li>
 * </ol>
 *
 * @author John Xing
 */
public class SimpleParser extends Configured {

  public static final Logger LOG = LoggerFactory.getLogger(SimpleParser.class);

  public static final String SHORTEST_URL = "http://t.tt/ttt";
  private CrawlFilters crawlFilters;
  private URLFilters urlFilters;
  private URLNormalizers urlNormalizers;
  private Map<String, Object> results = Maps.newHashMap();

  private String url;
  private WebPage page;
  private Parse parse;

  public SimpleParser(Configuration conf) {
    setConf(conf);
    crawlFilters = CrawlFilters.create(conf);
    urlFilters = new URLFilters(conf);
    urlNormalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
  }

  public void parse(String url) {
    parse(url, null);
  }

  public void parse(String url, String contentType) {
    LOG.info("parsing: " + url);

    if (url.length() < SHORTEST_URL.length()) {
      return;
    }

    try {
      this.url = url;
      page = download(url, contentType);
      if (page != null) {
        parse(page);
      }
    } catch (ProtocolNotFound e) {
      LOG.error(e.getMessage());
    }
  }

  public void parse(WebPage page) {
    try {
      ParseUtil parseUtil = new ParseUtil(getConf());
      if (page != null && page.getBaseUrl() != null) {
        String reverseUrl = TableUtil.reverseUrl(page.getBaseUrl().toString());
        parse = parseUtil.process(reverseUrl, page);
      }
    } catch (MalformedURLException e) {
      LOG.error(e.getMessage());
    }
  }

  public void extract(String url, String contentType) {
    LOG.info("parsing: " + url);

    try {
      this.url = url;
      page = download(url, contentType);
      if (page != null) {
        extract(page);
      }
    } catch (ProtocolNotFound|SAXException|IOException|ProcessingException e) {
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
    System.out.println(scentDoc.getText(true, true));
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

  public WebPage download(String url) throws ProtocolNotFound {
    return download(url, null);
  }

  public WebPage download(String url, String contentType) throws ProtocolNotFound {
    LOG.info("Fetching: " + url);

    ProtocolFactory factory = new ProtocolFactory(getConf());
    Protocol protocol = factory.getProtocol(url);
    WebPage page = WebPage.newBuilder().build();

    ProtocolOutput protocolOutput = protocol.getProtocolOutput(url, page);
    ProtocolStatus pstatus = protocolOutput.getStatus();

    if (!pstatus.isSuccess()) {
      LOG.error("Fetch failed with protocol status, "
          + ProtocolStatusUtils.getName(pstatus.getCode())
          + " : " + ProtocolStatusUtils.getMessage(pstatus));
      return null;
    }

    Content content = protocolOutput.getContent();

    FetchUtil.setStatus(page, CrawlStatus.STATUS_FETCHED, pstatus);
    FetchUtil.setContent(page, content);
    FetchUtil.setFetchTime(page, CrawlStatus.STATUS_FETCHED);
    FetchUtil.setMarks(page);

    if (content == null) {
      LOG.error("No content for " + url);
      return null;
    }

    saveWebPage(page);

    return page;
  }

  private void saveWebPage(WebPage page) {
    try {
      Path path = Paths.get("/tmp/nutch/web/" + DigestUtils.md5Hex(page.getBaseUrl().toString()));

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
    if (parse == null) {
      LOG.error("Problem with parse - check log");
      return results;
    }

    results.put("content", page.toString());

    if (ParserMapper.isTruncated(url, page)) {
      results.put("contentStatus", "truncated");
    }

    // Calculate the signature
    byte[] signature = SignatureFactory.getSignature(getConf()).calculate(page);

    LOG.info("signature: " + StringUtil.toHexString(signature));

    // LOG.info("---------\nMetadata\n---------\n");
    Map<CharSequence, ByteBuffer> metadata = page.getMetadata();
    StringBuffer sb = new StringBuffer();
    if (metadata != null) {
      Iterator<Entry<CharSequence, ByteBuffer>> iterator = metadata.entrySet().iterator();

      while (iterator.hasNext()) {
        Entry<CharSequence, ByteBuffer> entry = iterator.next();
        sb.append(entry.getKey().toString()).append(" : \t")
            .append(Bytes.toString(entry.getValue().array())).append("\n");
      }

      results.put("Metadata", sb);
    }

    Set<String> filteredOutlinks = Sets.newTreeSet();
    for (CharSequence link : page.getOutlinks().keySet()) {
      filteredOutlinks.add(link.toString());
    }
    results.put("Outlinks", StringUtils.join(filteredOutlinks, '\n'));

    // DiscardedOutlinks
    Set<String> discardedOutlinks = Sets.newTreeSet();
    for (Outlink outlink : parse.getOutlinks()) {
      String toUrl = outlink.getToUrl();
      toUrl = urlNormalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
      toUrl = urlFilters.filter(toUrl);
      if (toUrl == null) {
        discardedOutlinks.add(outlink.getToUrl());
      }
    }
    results.put("DiscardOutlinks", StringUtils.join(discardedOutlinks, '\n'));

    if (page.getHeaders() != null) {
      Map<CharSequence, CharSequence> headers = page.getHeaders();
      StringBuffer headersb = new StringBuffer();
      if (metadata != null) {
        Iterator<Entry<CharSequence, CharSequence>> iterator = headers.entrySet().iterator();

        while (iterator.hasNext()) {
          Entry<CharSequence, CharSequence> entry = iterator.next();
          headersb
              .append(entry.getKey().toString())
              .append(" : \t")
              .append(entry.getValue())
              .append("\n");
        }

        results.put("Headers", headersb);
      }
    }

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

    SimpleParser parser = new SimpleParser(NutchConfiguration.create());
    // parser.parse(url, contentType);
    parser.extract(url, contentType);

    System.out.println(parser.getResult());
  }
}
