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

package org.apache.nutch.parse.html;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.filter.CrawlFilter;
import org.apache.nutch.filter.CrawlFilters;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.*;
import org.apache.nutch.storage.gora.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.EncodingDetector;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.warps.scent.document.TextDocument;
import org.warps.scent.extractors.ChineseNewsExtractor;
import org.warps.scent.sax.SAXInput;
import org.warps.scent.util.ProcessingException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import static org.apache.nutch.metadata.Nutch.*;

public class HtmlParser implements Parser {

  public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.html");

  private static Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.BASE_URL);
  }

  private String parserImpl;
  private CrawlFilters crawlFilters;
  private String defaultCharEncoding;
  private Configuration conf;
  private RegexExtractor regexExtractor;
  private EncodingDetector encodingDetector;

  private DOMContentUtils domContentUtils;
  private ParseFilters htmlParseFilters;
  private String cachingPolicy;

  private DocumentFragment docRoot;

  private HTMLMetaTags metaTags = new HTMLMetaTags();
  private ArrayList<Outlink> outlinks = new ArrayList<>();

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.htmlParseFilters = new ParseFilters(getConf());
    this.parserImpl = getConf().get("parser.html.impl", "neko");
    this.defaultCharEncoding = getConf().get("parser.character.encoding.default", "windows-1252");
    this.domContentUtils = new DOMContentUtils(conf);
    this.cachingPolicy = getConf().get("parser.caching.forbidden.policy", CACHING_FORBIDDEN_CONTENT);
    this.crawlFilters = CrawlFilters.create(conf);
    this.regexExtractor = new RegexExtractor(conf);
    this.encodingDetector = new EncodingDetector(conf);

    LOG.info(Params.formatAsLine(
        "className", this.getClass().getSimpleName(),
        "parserImpl", parserImpl,
        "defaultCharEncoding", defaultCharEncoding,
        "cachingPolicy", cachingPolicy
    ));
  }

  public Parse getParse(String url, WebPage page) {
    URL baseURL;
    try {
      baseURL = new URL(TableUtil.toString(page.getBaseUrl()));
    } catch (MalformedURLException e) {
      return ParseStatusUtils.getEmptyParse(e, getConf());
    }

    InputSource input = getContentAsInputSource(page);
    String encoding = encodingDetector.sniffEncoding(page);
    setEncoding(page, encoding);
    input.setEncoding(encoding);
    docRoot = doParse(input);

    if (docRoot == null) {
      LOG.warn("Failed to parse document with encoding " + encoding + ", url : " + url);
      return ParseStatusUtils.getEmptyParse(null, getConf());
    }

    // Get meta directives
    HTMLMetaProcessor.getMetaTags(metaTags, docRoot, baseURL);
    setMetadata(page, metaTags);

    // Check meta directives
    if (!metaTags.getNoIndex()) { // okay to index
      // Get input source, again. It's not reusable
      InputSource input2 = getContentAsInputSource(page, encoding);
      extract(page, input2);
    }

    String pageTitle = page.getTitle() != null ? page.getTitle().toString() : "";
    String textContent = page.getTemporaryVariableAsString(DOC_FIELD_TEXT_CONTENT, "");

    tryGetValidOutlinks(page, url, baseURL);

    ParseStatus status = getStatus(metaTags);
    Parse parse = new Parse(textContent, pageTitle, outlinks.toArray(new Outlink[0]), status);
    parse = htmlParseFilters.filter(url, page, parse, metaTags, docRoot);
    if (parse == null) {
      LOG.debug("Parse filtered to null, url : " + url);
    }

    if (metaTags.getNoCache()) {
      // Not okay to cache
      page.putMetadata(CACHING_FORBIDDEN_KEY, cachingPolicy);
    }

    CrawlFilter.PageCategory pageCategory = CrawlFilter.sniffPageCategory(page);
    page.setPageCategory(pageCategory);
    if (pageCategory.isDetail()) {
      page.setPageCategoryLikelihood(0.9f);
    }

    return parse;
  }

  private DocumentFragment doParse(InputSource input) {
    try {
      return parse(input);
    } catch (Throwable e) {
      LOG.error("Failed to parse, message : {}", e);
    }

    return null;
  }

  private InputSource getContentAsInputSource(WebPage page, String encoding) {
    InputSource input = getContentAsInputSource(page);
    input.setEncoding(encoding);
    return input;
  }

  private InputSource getContentAsInputSource(WebPage page) {
    ByteBuffer contentInOctets = page.getContent();

    ByteArrayInputStream stream = new ByteArrayInputStream(contentInOctets.array(),
        contentInOctets.arrayOffset() + contentInOctets.position(),
        contentInOctets.remaining());

    return new InputSource(stream);
  }

  private ParseStatus getStatus(HTMLMetaTags metaTags) {
    ParseStatus status = ParseStatus.newBuilder().build();
    status.setMajorCode((int) ParseStatusCodes.SUCCESS);
    if (metaTags.getRefresh()) {
      status.setMinorCode((int) ParseStatusCodes.SUCCESS_REDIRECT);
      status.getArgs().add(new Utf8(metaTags.getRefreshHref().toString()));
      status.getArgs().add(new Utf8(Integer.toString(metaTags.getRefreshTime())));
    }

    return status;
  }

  private void tryGetValidOutlinks(WebPage page, String url, URL base) {
    /*
     * TODO : This is a temporary solution, and should be configured
     * */
    if (crawlFilters.veryLikelyBeSearchUrl(url)) {
      return;
    }

    if (!metaTags.getNoFollow()) { // okay to follow links
      outlinks.clear();

      URL baseTag = domContentUtils.getBase(docRoot);

      domContentUtils.getOutlinks(baseTag != null ? baseTag : base, outlinks, docRoot, crawlFilters);
    }

    page.setTmporaryVariable(VAR_OUTLINKS_COUNT, outlinks.size());

    if (LOG.isTraceEnabled()) {
      LOG.trace("found " + outlinks.size() + " outlinks in " + url);
    }
  }

  private void setEncoding(WebPage page, String encoding) {
    page.setTmporaryVariable("encoding", encoding);
    page.putMetadata(Nutch.ORIGINAL_CHAR_ENCODING, encoding);
    page.putMetadata(Nutch.CHAR_ENCODING_FOR_CONVERSION, encoding);
  }

  private void setMetadata(WebPage page, HTMLMetaTags metaTags) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Meta tags for " + page.getBaseUrl() + ": " + metaTags.toString());
    }

    Metadata metadata = metaTags.getGeneralTags();
    for (String name : metadata.names()) {
      page.putMetadata("meta_" + name, metadata.get(name));
    }
  }

  private void extract(WebPage page, InputSource input) {
    LOG.trace("Try extract by Scent");

    if (page.getContent() == null) {
      LOG.warn("Can not extract content, page content is null");
      return;
    }

    try {
      TextDocument doc = new SAXInput(input).getTextDocument();
      doc.setBaseUrl(page.getBaseUrl().toString());

      ChineseNewsExtractor extractor = new ChineseNewsExtractor();
      extractor.setRegexFieldRules(regexExtractor.getRegexFieldRules());
      extractor.setLabeledFieldRules(regexExtractor.getLabeledFieldRules());
      extractor.setTerminatingBlocksContains(regexExtractor.getTerminatingBlocksContains());
      extractor.setTerminatingBlocksStartsWith(regexExtractor.getTerminatingBlocksStartsWith());

      extractor.process(doc);

      page.setTitle(doc.getPageTitle());
      page.setTmporaryVariable(DOC_FIELD_TEXT_CONTENT, doc.getTextContent());
      page.setTmporaryVariable(DOC_FIELD_HTML_CONTENT, doc.getHtmlContent());
      doc.getFields().entrySet().forEach(entry -> page.setTmporaryVariable(entry.getKey(), entry.getValue()));

//      LOG.info("Text content length : " + doc.getTextContent().length()
//          + ", Html content length : " + doc.getHtmlContent().length() + ", url : " + page.getBaseUrl());

      page.setTextContentLength(doc.getTextContent().length());
      String publishTime = doc.getField(DOC_FIELD_PUBLISH_TIME);
      if (publishTime != null) {
        page.setPublishTime(publishTime);
      }

    } catch (ProcessingException|SAXException e) {
      LOG.warn("Failed to extract text content by boilerpipe, " + e.getMessage());
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<GoraWebPage.Field> getFields() {
    return FIELDS;
  }

  private DocumentFragment parse(InputSource input) throws Exception {
    if (parserImpl.equalsIgnoreCase("tagsoup"))
      return parseTagSoup(input);
    else
      return parseNeko(input);
  }

  private DocumentFragment parseTagSoup(InputSource input) throws Exception {
    HTMLDocumentImpl doc = new HTMLDocumentImpl();
    DocumentFragment frag = doc.createDocumentFragment();
    DOMBuilder builder = new DOMBuilder(doc, frag);
    org.ccil.cowan.tagsoup.Parser reader = new org.ccil.cowan.tagsoup.Parser();
    reader.setContentHandler(builder);
    reader.setFeature(org.ccil.cowan.tagsoup.Parser.ignoreBogonsFeature, true);
    reader.setFeature(org.ccil.cowan.tagsoup.Parser.bogonsEmptyFeature, false);
    reader.setProperty("http://xml.org/sax/properties/lexical-handler", builder);
    reader.parse(input);
    return frag;
  }

  private DocumentFragment parseNeko(InputSource input) throws Exception {
    org.cyberneko.html.parsers.DOMFragmentParser parser = new org.cyberneko.html.parsers.DOMFragmentParser();
    try {
      parser.setFeature("http://cyberneko.org/html/features/scanner/allow-selfclosing-iframe", true);
      parser.setFeature("http://cyberneko.org/html/features/augmentations", true);
      parser.setProperty("http://cyberneko.org/html/properties/default-encoding", defaultCharEncoding);
      parser.setFeature("http://cyberneko.org/html/features/scanner/ignore-specified-charset", true);
      parser.setFeature("http://cyberneko.org/html/features/balance-tags/ignore-outside-content", false);
      parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment", true);
      parser.setFeature("http://cyberneko.org/html/features/report-errors", LOG.isTraceEnabled());
    } catch (SAXException e) {
    }

    // convert Document to DocumentFragment
    HTMLDocumentImpl doc = new HTMLDocumentImpl();
    doc.setErrorChecking(false);
    DocumentFragment res = doc.createDocumentFragment();
    DocumentFragment frag = doc.createDocumentFragment();
    parser.parse(input, frag);
    res.appendChild(frag);

    try {
      while (true) {
        frag = doc.createDocumentFragment();
        parser.parse(input, frag);
        if (!frag.hasChildNodes())
          break;
        if (LOG.isInfoEnabled()) {
          LOG.info(" - new frag, " + frag.getChildNodes().getLength() + " nodes.");
        }
        res.appendChild(frag);
      }
    } catch (Exception x) {
      LOG.error("Failed with the following Exception: ", x);
    }

    return res;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage : HtmlParser webPageFile crawlFiltersFile");
      return;
    }

    // LOG.setLevel(Level.FINE);
    String name = args[0];
    String url = "file:" + name;
    File file = new File(name);
    byte[] bytes = new byte[(int) file.length()];
    DataInputStream in = new DataInputStream(new FileInputStream(file));
    in.readFully(bytes);

    Configuration conf = NutchConfiguration.create();

    String rules = new String(Files.readAllBytes(Paths.get(args[1])));
    conf.set(CrawlFilters.CRAWL_FILTER_RULES, rules);

    HtmlParser parser = new HtmlParser();
    parser.setConf(conf);
    WebPage page = WebPage.newWebPage();
    page.setBaseUrl(new Utf8(url));
    page.setContent(ByteBuffer.wrap(bytes));
    page.setContentType(new Utf8("text/html"));
    Parse parse = parser.getParse(url, page);

    System.out.println("title: " + parse.getTitle());
    System.out.println("text: " + parse.getText());
    System.out.println("outlinks: " + Arrays.toString(parse.getOutlinks()));
  }
}
