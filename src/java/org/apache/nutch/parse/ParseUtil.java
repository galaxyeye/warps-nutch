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

// Commons Logging imports

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.Signature;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.filter.CrawlFilters;
import org.apache.nutch.filter.URLFilterException;
import org.apache.nutch.filter.URLFilters;
import org.apache.nutch.filter.URLNormalizers;
import org.apache.nutch.jobs.fetch.FetchJob;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.ParseStatus;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nutch.metadata.Metadata.Name.REDIRECT_DISCOVERED;
import static org.apache.nutch.metadata.Nutch.*;

/**
 * A Utility class containing methods to simply perform parsing utilities such
 * as iterating through a preferred list of {@link Parser}s to obtain
 * {@link ParseResult} objects.
 *
 * @author mattmann
 * @author J&eacute;r&ocirc;me Charron
 * @author S&eacute;bastien Le Callonnec
 */
public class ParseUtil {

  /* our log stream */
  public static final Logger LOG = LoggerFactory.getLogger(ParseUtil.class);

  private static final int DEFAULT_MAX_PARSE_TIME = 30;

  private final Configuration conf;
  private final Set<CharSequence> unparsableTypes = new HashSet<>();
  private final Signature sig;
  private final URLFilters urlFilters;
  private final URLNormalizers urlNormalizers;
  private final CrawlFilters crawlFilters;
  private final URLUtil.HostGroupMode hostGroupMode;
  private final int maxParsedOutlinks;
  private final boolean ignoreExternalLinks;
  private final int minAnchorLength;
  private final ParserFactory parserFactory;
  /**
   * Parser timeout set to 30 sec by default. Set -1 to deactivate
   **/
  private final int maxParseTime;
  private final ExecutorService executorService;

  /**
   * @param conf
   */
  public ParseUtil(Configuration conf) {
    this.conf = conf;
    parserFactory = new ParserFactory(conf);
    maxParseTime = conf.getInt("parser.timeout", DEFAULT_MAX_PARSE_TIME);
    sig = SignatureFactory.getSignature(conf);
    urlFilters = new URLFilters(conf);
    urlNormalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
    crawlFilters = CrawlFilters.create(conf);
    hostGroupMode = conf.getEnum(PARAM_FETCH_QUEUE_MODE, URLUtil.HostGroupMode.BY_HOST);
    maxParsedOutlinks = ConfigUtils.getUintOrMax(conf, PARAM_PARSE_MAX_OUTLINKS_PER_PAGE, 200);
    ignoreExternalLinks = conf.getBoolean(PARAM_PARSE_IGNORE_EXTERNAL_LINKS, true);
    minAnchorLength = conf.getInt(PARAM_PARSE_MIN_ANCHOR_LENGTH, 8);
    executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("parse-%d").setDaemon(true).build());
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Performs a parse by iterating through a List of preferred {@link Parser}s
   * until a successful parse is performed and a {@link ParseResult} object is
   * returned. If the parse is unsuccessful, a message is logged to the
   * <code>WARNING</code> level, and an empty parse is returned.
   *
   * @throws ParseException If there is an error parsing.
   */
  public ParseResult parse(String url, WebPage page) throws ParseException {
    Parser[] parsers;

    String contentType = page.getContentType();

    parsers = this.parserFactory.getParsers(contentType, url);

    for (int i = 0; i < parsers.length; i++) {
      // LOG.debug("Parsing [" + url + "] with [" + parsers[i] + "]");
      ParseResult parseResult;

      if (maxParseTime != -1) {
        parseResult = runParser(parsers[i], url, page);
      } else {
        parseResult = parsers[i].getParse(url, page);
      }

      if (parseResult != null && ParseStatusUtils.isSuccess(parseResult.getParseStatus())) {
        return parseResult;
      }
    }

    LOG.debug("Failed to parse " + url + " with type " + contentType);

    return ParseStatusUtils.getEmptyParse(new ParseException("Unable to parse content"), null);
  }

  /**
   * Parses given web page and stores parsed content within page. Puts a
   * meta-redirect to outlinks.
   *
   * @param url
   *          The url
   * @param page
   *          The web page
   */
  public ParseResult process(String url, WebPage page) {
    byte status = page.getStatus().byteValue();
    if (status != CrawlStatus.STATUS_FETCHED) {
      return null;
    }

    ParseResult parseResult;
    try {
      parseResult = parse(url, page);
    } catch (ParserNotFound e) {
      String contentType = page.getContentType();
      if (!unparsableTypes.contains(contentType)) {
        unparsableTypes.add(contentType);
        LOG.warn("No suitable parser found: " + e.getMessage());
      }
      return null;
    } catch (final Exception e) {
      LOG.warn("Failed to parseResult : " + url + ": " + StringUtils.stringifyException(e));
      return null;
    }

    if (parseResult == null) {
      return null;
    }

    ParseStatus pstatus = parseResult.getParseStatus();
    page.setParseStatus(pstatus);
    if (ParseStatusUtils.isSuccess(pstatus)) {
      if (pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        processRedirect(url, page, pstatus);
      } else {
        parseResult = processSuccess(url, page, parseResult);
      }
    }

    return parseResult;
  }

  public Set<CharSequence> getUnparsableTypes() { return unparsableTypes; }

  private ParseResult runParser(Parser p, String url, WebPage page) {
    ParseCallable pc = new ParseCallable(p, page, url);
    Future<ParseResult> task = executorService.submit(pc);

    ParseResult res = null;
    try {
      res = task.get(maxParseTime, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Failed to parsing " + url, e);
      task.cancel(true);
    }

    return res;
  }

  private ParseResult processSuccess(String url, WebPage page, ParseResult parseResult) {
    page.setPageTitle(parseResult.getPageTitle());
    page.setPageText(parseResult.getPageText());

    ByteBuffer prevSig = page.getSignature();
    if (prevSig != null) {
      page.setPrevSignature(prevSig);
    }
    page.setSignature(sig.calculate(page));

    // Collect out-links
    // TODO : use no-follow
    boolean follow = page.isSeed() || page.getPageCategory().isIndex();
    if (follow) {
      final String sourceHost = ignoreExternalLinks ? null : URLUtil.getHost(url, hostGroupMode);
      final String oldOutLinks = page.getAllOutLinks();
      Map<CharSequence, CharSequence> outlinks = parseResult.getOutlinks().stream()
          .filter(ol -> validateOutLink(ol, sourceHost, oldOutLinks)) // filter out in-valid urls
          .sorted((l1, l2) -> l2.getAnchor().length() - l1.getAnchor().length()) // longer anchor comes first
          .limit(maxParsedOutlinks)
          .collect(Collectors.toMap(Outlink::getToUrl, Outlink::getAnchor, (v1, v2) -> v1.length() > v2.length() ? v1 : v2));
      page.setOutlinks(outlinks);

      if (page.getAllOutLinks().length() > (10 * 1000) * 100) {
        LOG.warn("Too long old out links string, url : " + url);
      }
      page.addToAllOutLinks(outlinks.keySet());
    }

    // TODO : Marks should be set in mapper or reducer, not util methods
    page.putMarkIfNonNull(Mark.PARSE, page.getMark(Mark.FETCH));

    return parseResult;
  }

  private void processRedirect(String url, WebPage page, ParseStatus pstatus) {
    String newUrl = ParseStatusUtils.getMessage(pstatus);
    try {
      newUrl = urlNormalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      newUrl = urlFilters.filter(newUrl);
    } catch (MalformedURLException|URLFilterException e) {
      LOG.warn("Malformed url exception parsing redirect " + url);
      newUrl = null;
    }

    if (newUrl == null) {
      return;
    }

    page.getOutlinks().put(new Utf8(newUrl), new Utf8());

    page.putMetadata(REDIRECT_DISCOVERED, YES_STRING);

    if (newUrl.equals(url)) {
      int refreshTime = StringUtil.tryParseInt(ParseStatusUtils.getArg(pstatus, 1), 0);
      String reprUrl = URLUtil.chooseRepr(url, newUrl, refreshTime < FetchJob.PERM_REFRESH_TIME);
      if (reprUrl != null) {
        page.setReprUrl(reprUrl);
      }
    }
  }

  private boolean validateOutLink(Outlink link, String sourceHost, String oldOutLinks) {
    if (link.getToUrl().isEmpty() || sourceHost.isEmpty()) {
      return false;
    }

    if (link.getAnchor().length() < minAnchorLength) {
      return false;
    }

    if (oldOutLinks.contains(link.getToUrl() + "\n")) {
      return false;
    }

    String toUrl = crawlFilters.normalizeUrlToEmpty(link.getToUrl());
    if (toUrl.isEmpty()) {
      return false;
    }

    if (oldOutLinks.contains(toUrl + "\n")) {
      return false;
    }

    String destHost = URLUtil.getHost(toUrl, hostGroupMode);
    if (destHost == null) {
      return false;
    }

    if (ignoreExternalLinks && !sourceHost.equals(destHost)) {
      return false;
    }

    return true;
  }
}
