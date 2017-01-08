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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.Signature;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.filter.*;
import org.apache.nutch.jobs.fetch.FetchJob;
import org.apache.nutch.persist.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.ParseStatus;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nutch.metadata.Metadata.Name.REDIRECT_DISCOVERED;
import static org.apache.nutch.metadata.Nutch.PARAM_FETCH_QUEUE_MODE;
import static org.apache.nutch.metadata.Nutch.YES_STRING;

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
  private final int maxOutlinks;
  private final Random rand = new Random(System.currentTimeMillis());
  private final boolean ignoreExternalLinks;
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
    int maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 1000);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE : maxOutlinksPerPage;
    ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
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
        processSuccess(url, page, parseResult);
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

  private void processSuccess(String url, WebPage page, ParseResult parseResult) {
    page.setText(parseResult.getText());
    page.setTitle(parseResult.getTitle());

    ByteBuffer prevSig = page.getSignature();
    if (prevSig != null) {
      page.setPrevSignature(prevSig);
    }
    page.setSignature(sig.calculate(page));

    if (!CrawlFilter.sniffPageCategory(url).isDetail()) {
      final String sourceHost = ignoreExternalLinks ? null : URLUtil.getHost(url, hostGroupMode);
      List<Outlink> outLinks = Lists.newLinkedList(parseResult.getOutlinks());
      // Shuffle the links so they have a fair chance to be collected
      Collections.shuffle(outLinks, rand);
      Map<CharSequence, CharSequence> outlinks = outLinks.stream()
          .filter(l -> validateOutLink(sourceHost, l))
          .limit(maxOutlinks)
          .collect(Collectors.toMap(Outlink::getToUrl, Outlink::getAnchor, (v1, v2) -> v1.length() > v2.length() ? v1 : v2));
      page.setOutlinks(outlinks);
    }

    // TODO : Marks should be set in mapper or reducer, not util methods
    page.putMarkIfNonNull(Mark.PARSE, page.getMark(Mark.FETCH));
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

  private boolean validateOutLink(String sourceHost, Outlink link) {
    String toUrl = crawlFilters.normalizeUrlToEmpty(link.getToUrl());
    return validateHosts(sourceHost, URLUtil.getHost(toUrl, hostGroupMode));
  }

  private boolean validateHosts(String sourceHost, String destHost) {
    return sourceHost != null && destHost != null && (!ignoreExternalLinks || destHost.equals(sourceHost));
  }
}
