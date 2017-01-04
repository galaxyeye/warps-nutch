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
import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.persist.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.ParseStatus;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.jetbrains.annotations.Contract;
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
import java.util.stream.Stream;

import static org.apache.nutch.filter.CrawlFilter.MEDIA_URL_SUFFIXES;
import static org.apache.nutch.metadata.Nutch.PARAM_FETCH_QUEUE_MODE;
import static org.apache.nutch.metadata.Nutch.YES_STRING;

/**
 * A Utility class containing methods to simply perform parsing utilities such
 * as iterating through a preferred list of {@link Parser}s to obtain
 * {@link Parse} objects.
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
    int maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE : maxOutlinksPerPage;
    ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("parse-%d").setDaemon(true).build());
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Performs a parse by iterating through a List of preferred {@link Parser}s
   * until a successful parse is performed and a {@link Parse} object is
   * returned. If the parse is unsuccessful, a message is logged to the
   * <code>WARNING</code> level, and an empty parse is returned.
   *
   * @throws ParseException If there is an error parsing.
   */
  public Parse parse(String url, WebPage page) throws ParseException {
    Parser[] parsers;

    String contentType = page.getContentType();

    parsers = this.parserFactory.getParsers(contentType, url);

    for (int i = 0; i < parsers.length; i++) {
      // LOG.debug("Parsing [" + url + "] with [" + parsers[i] + "]");
      Parse parse;

      if (maxParseTime != -1) {
        parse = runParser(parsers[i], url, page);
      } else {
        parse = parsers[i].getParse(url, page);
      }

      if (parse != null && ParseStatusUtils.isSuccess(parse.getParseStatus())) {
        return parse;
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
   * @param page
   */
  public Parse process(String url, WebPage page) {
    byte status = page.getStatus().byteValue();
    if (status != CrawlStatus.STATUS_FETCHED) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + url + ", status : " + CrawlStatus.getName(status));
      }
      return null;
    }

    Parse parse;
    try {
      parse = parse(url, page);
    } catch (ParserNotFound e) {
      String contentType = page.getContentType();
      if (!unparsableTypes.contains(contentType)) {
        unparsableTypes.add(contentType);
        LOG.warn("No suitable parser found: " + e.getMessage());
      }
      return null;
    } catch (final Exception e) {
      LOG.warn("Failed to parse : " + url + ": " + StringUtils.stringifyException(e));
      return null;
    }

    if (parse == null) {
      return null;
    }

    ParseStatus pstatus = parse.getParseStatus();
    page.setParseStatus(pstatus);
    if (ParseStatusUtils.isSuccess(pstatus)) {
      if (pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        processRedirect(url, page, pstatus);
      } else {
        processSuccess(url, page, parse);
      }
    }

    return parse;
  }

  public Set<CharSequence> getUnparsableTypes() { return unparsableTypes; }

  private Parse runParser(Parser p, String url, WebPage page) {
    ParseCallable pc = new ParseCallable(p, page, url);
    Future<Parse> task = executorService.submit(pc);

    Parse res = null;
    try {
      res = task.get(maxParseTime, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Failed to parsing " + url, e);
      task.cancel(true);
    }

    return res;
  }

  private void processSuccess(String url, WebPage page, Parse parse) {
    page.setText(parse.getText());
    page.setTitle(parse.getTitle());
    ByteBuffer prevSig = page.getSignature();
    if (prevSig != null) {
      page.setPrevSignature(prevSig);
    }

    page.setSignature(sig.calculate(page));

    // TODO : preformance issue
    String sourceHost = ignoreExternalLinks ? null : URLUtil.getHost(url, hostGroupMode);
    Map<CharSequence, CharSequence> outlinks = Stream.of(parse.getOutlinks())
        .map(l -> Pair.of(new Utf8(normalizeUrlToEmpty(page, l.getToUrl())), new Utf8(l.getAnchor())))
        .filter(pair -> pair.getKey().length() != 0)
        .distinct()
        .limit(maxOutlinks)
        .filter(pair -> validateDestHost(sourceHost, URLUtil.getHost(pair.getKey(), hostGroupMode)))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1.length() > v2.length() ? v1 : v2));
    page.setOutlinks(outlinks);

    // TODO : Marks should be set in mapper or reducer, not util methods
    page.putMarkIfNonNull(Mark.PARSE, page.getMark(Mark.FETCH));
  }

  private void processRedirect(String url, WebPage page, ParseStatus pstatus) {
    String newUrl = ParseStatusUtils.getMessage(pstatus);
    int refreshTime = StringUtil.tryParseInt(ParseStatusUtils.getArg(pstatus, 1), 0);
    try {
      newUrl = urlNormalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      if (newUrl == null) {
        LOG.warn("Redirect normalized to null " + url);
        return;
      }

      try {
        newUrl = urlFilters.filter(newUrl);
      } catch (URLFilterException e) {
        return;
      }

      if (newUrl == null) {
        LOG.warn("Redirect filtered to null " + url);
        return;
      }
    } catch (MalformedURLException e) {
      LOG.warn("Malformed url exception parsing redirect " + url);
      return;
    }

    page.getOutlinks().put(new Utf8(newUrl), new Utf8());

    // page.putMetadata(FetchJob.REDIRECT_DISCOVERED, Nutch.YES_VAL);
    page.putMetadata(Metadata.Name.REDIRECT_DISCOVERED, YES_STRING);

    if (newUrl.equals(url)) {
      String reprUrl = URLUtil.chooseRepr(url, newUrl, refreshTime < FetchJob.PERM_REFRESH_TIME);

      if (reprUrl == null) {
        LOG.warn("Null repr url for " + url);
      } else {
        page.setReprUrl(reprUrl);
      }
    }
  }

  private boolean validateDestHost(String sourceHost, String destHost) {
    return sourceHost != null && destHost != null && (!ignoreExternalLinks || destHost.equals(sourceHost));
  }

  @Contract("_, null -> !null")
  private String normalizeUrlToEmpty(WebPage page, final String url) {
    if (url == null) {
      return "";
    }

    // TODO : use suffix-urlfilter instead, this is a quick dirty fix
    if (Stream.of(MEDIA_URL_SUFFIXES).anyMatch(url::endsWith)) {
      return "";
    }

    String filteredUrl = temporaryUrlFilter(url);
    if (filteredUrl == null) {
      return "";
    }

    // We explicitly follow detail urls from seed page
    // TODO : this can be avoid
    if (page.isSeed() && crawlFilters.veryLikelyBeDetailUrl(filteredUrl)) {
      return filteredUrl;
    }

    try {
      filteredUrl = urlFilters.filter(filteredUrl);
      filteredUrl = urlNormalizers.normalize(url, URLNormalizers.SCOPE_OUTLINK);
    } catch (URLFilterException|MalformedURLException e) {
      LOG.error(e.toString());
    }

    return filteredUrl == null ? "" : filteredUrl;
  }

  private String temporaryUrlFilter(String url) {
    if (crawlFilters.veryLikelyBeSearchUrl(url) || crawlFilters.veryLikelyBeMediaUrl(url)) {
      url = "";
    }

    if (crawlFilters.containsOldDateString(url)) {
      url = "";
    }

    return url;
  }
}
