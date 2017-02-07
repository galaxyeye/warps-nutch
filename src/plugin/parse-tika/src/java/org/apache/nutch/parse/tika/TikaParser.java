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
package org.apache.nutch.parse.tika;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.parse.*;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.persist.gora.ParseStatus;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.MimeUtil;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import static org.apache.nutch.metadata.Nutch.CACHING_FORBIDDEN_CONTENT;
import static org.apache.nutch.metadata.Nutch.CACHING_FORBIDDEN_KEY;

/**
 * Wrapper for Tika parsers. Mimics the HTMLParser but using the XHTML
 * representation returned by Tika as SAX events
 ***/
public class TikaParser implements org.apache.nutch.parse.Parser {

  public static final Logger LOG = LoggerFactory.getLogger(TikaParser.class);

  private static Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.BASE_URL);
    FIELDS.add(GoraWebPage.Field.CONTENT_TYPE);
  }

  private Configuration conf;
  private TikaConfig tikaConfig = null;
  private DOMContentUtils utils;
  private ParseFilters htmlParseFilters;
  private String cachingPolicy;

  private HtmlMapper HTMLMapper;

  public TikaParser() {}

  public TikaParser(Configuration conf) {
    setConf(conf);
  }

  @Override
  public ParseResult getParse(String url, WebPage page) {
    String baseUrl = page.getBaseUrl();
    URL base;
    try {
      base = new URL(baseUrl);
    } catch (MalformedURLException e) {
      return ParseStatusUtils.getEmptyParse(e, getConf());
    }

    // get the right parser using the mime type as a clue
    String mimeType = page.getContentType();
    Parser parser = tikaConfig.getParser(mimeType);

    if (parser == null) {
      String message = "Can't retrieve Tika parser for mime-type " + mimeType;
      LOG.error(message);
      return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED_EXCEPTION, message, getConf());
    }

    // LOG.debug("Using Tika parser " + parser.getClass().getName() + " for mime-type " + mimeType);

    Metadata tikamd = new Metadata();

    HTMLDocumentImpl doc = new HTMLDocumentImpl();
    doc.setErrorChecking(false);
    DocumentFragment root = doc.createDocumentFragment();
    DOMBuilder domhandler = new DOMBuilder(doc, root);
    ParseContext context = new ParseContext();
    if (HTMLMapper != null) {
      context.set(HtmlMapper.class, HTMLMapper);
    }
    // to add once available in Tika
    // context.set(HtmlMapper.class, IdentityHtmlMapper.INSTANCE);
    tikamd.set(Metadata.CONTENT_TYPE, mimeType);
    try {
      ByteBuffer raw = page.getContent();
      parser.parse(new ByteArrayInputStream(raw.array(), raw.arrayOffset()
          + raw.position(), raw.remaining()), domhandler, tikamd, context);
    } catch (Exception e) {
      LOG.error("Error parsing " + url, e);
      return ParseStatusUtils.getEmptyParse(e, getConf());
    }

    HTMLMetaTags metaTags = new HTMLMetaTags();
    String text = "";
    String pageTitle = "";
    ArrayList<Outlink> outlinks = new ArrayList<>();

    // we have converted the sax events generated by Tika into a DOM object
    // so we can now use the usual HTML resources from Nutch
    // get meta directives
    HTMLMetaProcessor.getMetaTags(metaTags, root, base);

    // check meta directives
    if (!metaTags.getNoIndex()) { // okay to index
      StringBuffer sb = new StringBuffer();
      // TODO : Move the extraction to text-scent
      utils.getText(sb, root); // extract text
      text = sb.toString();
      sb.setLength(0);
      utils.getTitle(sb, root); // extract title
      pageTitle = sb.toString().trim();
    }

    if (!metaTags.getNoFollow()) { // okay to follow links
      URL baseTag = utils.getBase(root);
      utils.getOutlinks(baseTag != null ? baseTag : base, outlinks, root);
    }

    // populate Nutch metadata with Tika metadata
    for (String name : tikamd.names()) {
      if (name.equalsIgnoreCase(TikaCoreProperties.TITLE.toString())) {
        continue;
      }
      page.putMetadata(name, tikamd.get(name));
    }

    // no outlinks? try OutlinkExtractor e.g works for mime types where no
    // explicit markup for anchors

    if (outlinks.isEmpty()) {
      outlinks = OutlinkExtractor.getOutlinks(text, getConf());
    }

    ParseStatus status = ParseStatusUtils.STATUS_SUCCESS;
    if (metaTags.getRefresh()) {
      status.setMinorCode((int) ParseStatusCodes.SUCCESS_REDIRECT);
      status.getArgs().add(new Utf8(metaTags.getRefreshHref().toString()));
      status.getArgs().add(new Utf8(Integer.toString(metaTags.getRefreshTime())));
    }

    ParseResult parseResult = new ParseResult(text, pageTitle, outlinks, status);
    parseResult = htmlParseFilters.filter(url, page, parseResult, metaTags, root);

    if (metaTags.getNoCache()) { // not okay to cache
      page.putMetadata(CACHING_FORBIDDEN_KEY, cachingPolicy);
    }

    return parseResult;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.tikaConfig = null;

    try {
      tikaConfig = TikaConfig.getDefaultConfig();
    } catch (Exception e2) {
      String message = "Problem loading default Tika configuration";
      LOG.error(message, e2);
      throw new RuntimeException(e2);
    }

    // use a custom htmlmapper
    String htmlmapperClassName = conf.get("tika.htmlmapper.classname");
    if (StringUtils.isNotBlank(htmlmapperClassName)) {
      try {
        Class HTMLMapperClass = Class.forName(htmlmapperClassName);
        boolean interfaceOK = HtmlMapper.class.isAssignableFrom(HTMLMapperClass);
        if (!interfaceOK) {
          throw new RuntimeException("Class " + htmlmapperClassName + " does not implement HtmlMapper");
        }
        HTMLMapper = (HtmlMapper) HTMLMapperClass.newInstance();
      } catch (Exception e) {
        LOG.error("Can't generate instance for class " + htmlmapperClassName);
        throw new RuntimeException("Can't generate instance for class " + htmlmapperClassName);
      }
    }

    this.htmlParseFilters = new ParseFilters(getConf());
    this.utils = new DOMContentUtils(conf);
    this.cachingPolicy = getConf().get("parser.caching.forbidden.policy", CACHING_FORBIDDEN_CONTENT);
  }

  public TikaConfig getTikaConfig() {
    return this.tikaConfig;
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<GoraWebPage.Field> getFields() {
    return FIELDS;
  }

  // main class used for debuggin
  public static void main(String[] args) throws Exception {
    String name = args[0];
    String url = "file:" + name;
    File file = new File(name);
    byte[] bytes = new byte[(int) file.length()];
    @SuppressWarnings("resource")
    DataInputStream in = new DataInputStream(new FileInputStream(file));
    in.readFully(bytes);
    Configuration conf = ConfigUtils.create();
    // TikaParser parser = new TikaParser();
    // parser.setConf(conf);
    WebPage page = WebPage.newWebPage();
    page.setBaseUrl(url);
    page.setContent(bytes);
    MimeUtil mimeutil = new MimeUtil(conf);
    String mtype = mimeutil.getMimeType(file);
    page.setContentType(mtype);
    // ParseResult parseResult = parser.getParse(url, page);

    ParseResult parseResult = new ParseUtil(conf).parse(url, page);

    System.out.println("content type: " + mtype);
    System.out.println("title: " + parseResult.getPageTitle());
    System.out.println("text: " + parseResult.getText());
    System.out.println("outlinks: " + StringUtils.join(parseResult.getOutlinks(), ","));
  }
}
