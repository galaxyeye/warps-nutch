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
import org.apache.nutch.fetcher.FetchJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
  private final Signature sig;
  private final URLFilters filters;
  private final URLNormalizers normalizers;
  private final int maxOutlinks;
  private final boolean ignoreExternalLinks;
  private final ParserFactory parserFactory;
  /** Parser timeout set to 30 sec by default. Set -1 to deactivate **/
  private final int maxParseTime;
  private final ExecutorService executorService;

  /**
   * 
   * @param conf
   */
  public ParseUtil(Configuration conf) {
    this.conf = conf;
    parserFactory = new ParserFactory(conf);
    maxParseTime = conf.getInt("parser.timeout", DEFAULT_MAX_PARSE_TIME);
    sig = SignatureFactory.getSignature(conf);
    filters = new URLFilters(conf);
    normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
    int maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
        : maxOutlinksPerPage;
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
   * @throws ParserNotFound
   *           If there is no suitable parser found.
   * @throws ParseException
   *           If there is an error parsing.
   */
  public Parse parse(String url, WebPage page) throws ParserNotFound, ParseException {
    Parser[] parsers;

    String contentType = TableUtil.toString(page.getContentType());

    parsers = this.parserFactory.getParsers(contentType, url);

    for (int i = 0; i < parsers.length; i++) {
      if (LOG.isDebugEnabled()) {
        // LOG.debug("Parsing [" + url + "] with [" + parsers[i] + "]");
      }
      Parse parse;

      if (maxParseTime != -1) {
        parse = runParser(parsers[i], url, page);
      }
      else {
        parse = parsers[i].getParse(url, page);
      }

      if (parse != null && ParseStatusUtils.isSuccess(parse.getParseStatus())) {
        return parse;
      }
    }

    LOG.debug("Failed to parse " + url + " with type " + contentType);

    return ParseStatusUtils.getEmptyParse(new ParseException("Unable to parse content"), null);
  }

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

  /**
   * Parses given web page and stores parsed content within page. Puts a
   * meta-redirect to outlinks.
   * 
   * @param key
   * @param page
   */
  public Parse process(String key, WebPage page) {
    String url = TableUtil.unreverseUrl(key);
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
      // do not print stacktrace for the fact that some types are not mapped.
      LOG.warn("No suitable parser found: " + e.getMessage());
      return null;
    } catch (final Exception e) {
      LOG.warn("Failed to parse : " + url + ": " + StringUtils.stringifyException(e));
      return null;
    }

    if (parse == null) {
      return null;
    }

    org.apache.nutch.storage.ParseStatus pstatus = parse.getParseStatus();
    page.setParseStatus(pstatus);
    if (ParseStatusUtils.isSuccess(pstatus)) {
      if (pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        processRedirect(url, page, pstatus);
      } else {
        processSuccess(url, page, pstatus, parse);
      }
    }

    return parse;
  }

  private void processSuccess(String url, WebPage page, org.apache.nutch.storage.ParseStatus pstatus, Parse parse) {
    page.setText(new Utf8(parse.getText()));
    page.setTitle(new Utf8(parse.getTitle()));
    ByteBuffer prevSig = page.getSignature();
    if (prevSig != null) {
      page.setPrevSignature(prevSig);
    }

    final byte[] signature = sig.calculate(page);
    page.setSignature(ByteBuffer.wrap(signature));
    if (page.getOutlinks() != null) {
      page.getOutlinks().clear();
    }

    final Outlink[] outlinks = parse.getOutlinks();
    int outlinksToStore = Math.min(maxOutlinks, outlinks.length);
    String fromHost;
    if (ignoreExternalLinks) {
      try {
        fromHost = new URL(url).getHost().toLowerCase();
      } catch (final MalformedURLException e) {
        fromHost = null;
      }
    } else {
      fromHost = null;
    }

    int validCount = 0;
    for (int i = 0; validCount < outlinksToStore && i < outlinks.length; i++) {
      String toUrl = outlinks[i].getToUrl();
      try {
        toUrl = normalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
        toUrl = filters.filter(toUrl);
      } catch (MalformedURLException e2) {
        continue;
      } catch (URLFilterException e) {
        continue;
      }

      if (toUrl == null) {
        // LOG.debug("Violate filter or normalizers");
        continue;
      }

      Utf8 utf8ToUrl = new Utf8(toUrl);
      if (page.getOutlinks().get(utf8ToUrl) != null) {
        // skip duplicate outlinks
        continue;
      }

      String toHost;
      if (ignoreExternalLinks) {
        try {
          toHost = new URL(toUrl).getHost().toLowerCase();
        } catch (final MalformedURLException e) {
          toHost = null;
        }

        if (toHost == null || !toHost.equals(fromHost)) { // external links
          continue; // skip it
        }
      }

      validCount++;
      page.getOutlinks().put(utf8ToUrl, new Utf8(outlinks[i].getAnchor()));
    } // for

    Utf8 fetchMark = Mark.FETCH_MARK.checkMark(page);
    if (fetchMark != null) {
      Mark.PARSE_MARK.putMark(page, fetchMark);
    }
  }

  private void processRedirect(String url, WebPage page, org.apache.nutch.storage.ParseStatus pstatus) {
    String newUrl = ParseStatusUtils.getMessage(pstatus);
    int refreshTime = Integer.parseInt(ParseStatusUtils.getArg(pstatus, 1));
    try {
      newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      if (newUrl == null) {
        LOG.warn("Redirect normalized to null " + url);
        return;
      }

      try {
        newUrl = filters.filter(newUrl);
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

    TableUtil.putMetadata(page, FetchJob.REDIRECT_DISCOVERED, Nutch.YES_VAL);

    if (newUrl == null || newUrl.equals(url)) {
      String reprUrl = URLUtil.chooseRepr(url, newUrl, refreshTime < FetchJob.PERM_REFRESH_TIME);

      if (reprUrl == null) {
        LOG.warn("Null repr url for " + url);
        return;
      } else {
        page.setReprUrl(new Utf8(reprUrl));
      }
    }
  }
}
