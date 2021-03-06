package org.apache.nutch.dbupdate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.WebPageWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.ObjectCache;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * Created by vincent on 16-9-25.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class MapDatumBuilder {

  public static final Logger LOG = LoggerFactory.getLogger(MapDatumBuilder.class);

  public static final String URL_FILTERING = "dbupdate.url.filters";
  public static final String URL_NORMALIZING = "dbupdate.url.normalizers";
  public static final String URL_NORMALIZING_SCOPE = "dbupdate.url.normalizers.scope";

  private NutchCounter counter;
  private Configuration conf;

  private final String batchId;
  private final int maxDistance;
  private final boolean normalize;
  private final boolean filter;
  private final URLNormalizers normalizers;
  private final URLFilters urlFilters;
  private final CrawlFilters crawlFilters;
  private ScoringFilters scoringFilters;

  public MapDatumBuilder(NutchCounter counter, Configuration conf) {
    this.counter = counter;
    this.conf = conf;

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);

    maxDistance = conf.getInt(PARAM_GENERATOR_MAX_DISTANCE, -1);
    batchId = conf.get(PARAM_BATCH_ID, ALL_BATCH_ID_STR);
    normalize = conf.getBoolean(URL_NORMALIZING, true);
    filter = conf.getBoolean(URL_FILTERING, true);
    normalizers = normalize ? new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK) : null;
    urlFilters = filter ? new URLFilters(conf) : null;
    crawlFilters = CrawlFilters.create(conf);
    scoringFilters = new ScoringFilters(conf);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "normalize", normalize,
        "filter", filter,
        "maxDistance", maxDistance
    ));
  }

  public boolean isValidUrl(WebPage page, CharSequence url) {
    return filterUrl(page, url.toString()) != null;
  }

  // TODO : make sure url == page.getBaseUrl()
  public String filterUrl(WebPage mainPage, String url) {
    try {
      if (normalize) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_OUTLINK);
      }

      if (filter) {
        url = temporaryUrlFilter(url, mainPage);

        // We explicitly follow detail urls from seed page TODO : this can be avoid
        if (TableUtil.isSeed(mainPage) && crawlFilters.isDetailUrl(url)) {
          return url;
        }

        url = urlFilters.filter(url);
      }
    } catch (URLFilterException | MalformedURLException e) {
      LOG.error(e.toString());
    }

    return url;
  }

  // TODO : make it a plugin
  private String temporaryUrlFilter(String url, WebPage mainPage) {
    if (crawlFilters.isSearchUrl(url) || crawlFilters.isMediaUrl(url)) {
      url = null;
    }

    if (crawlFilters.containsOldDateString(url)) {
      url = null;
    }

    return url;
  }

  /**
   * Build map phrase datum
   * */
  public Pair<UrlWithScore, NutchWritable> buildMainDatum(String reversedUrl, WebPage page) {
    NutchWritable nutchWritable = new NutchWritable();
    nutchWritable.set(new WebPageWritable(conf, page));
    return Pair.of(new UrlWithScore(reversedUrl, Float.MAX_VALUE), nutchWritable);
  }

  /**
   * Build map phrase datum
   * */
  public Pair<UrlWithScore, NutchWritable> createNewDatum(String url, float score, WebPage sourcePage) {
    String reversedUrl = null;
    try {
      reversedUrl = TableUtil.reverseUrl(url);
    } catch (MalformedURLException ignored) {
    }

    NutchWritable nutchWritable = new NutchWritable();
    nutchWritable.set(new WebPageWritable(conf, sourcePage));
    return Pair.of(new UrlWithScore(reversedUrl, score), nutchWritable);
  }

  public List<Outlink> getFilteredOutlinks(WebPage sourcePage, final int limit) {
    List<Outlink> orderedOutlinks = new LinkedList<>();

    Object obj = sourcePage.getTemporaryVariable(VAR_ORDERED_OUTLINKS);
    if (obj != null && obj instanceof Outlink[]) {
      orderedOutlinks = Stream.of((Outlink[])obj).collect(Collectors.toList());
    }
    else {
      final Map<CharSequence, CharSequence> unorderedOutlinks = sourcePage.getOutlinks();
      if (unorderedOutlinks != null) {
        orderedOutlinks = unorderedOutlinks.entrySet().stream()
            .map(e -> new Outlink(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
      }
    }

    return orderedOutlinks.stream()
        .filter(e -> isValidUrl(sourcePage, e.getToUrl()))
        .limit(limit)
        .collect(Collectors.toList());
  }

  /**
   * Generate new Pages from outlinks
   *
   * TODO : Write the result into hdfs directly to deduce memory consumption
   * */
  public Map<UrlWithScore, NutchWritable> createRowsFromOutlink(String sourceUrl, WebPage sourcePage) {
    final int depth = TableUtil.getDistance(sourcePage);

    if (depth >= maxDistance) {
      counter.increase(NutchCounter.Counter.tooDeepPages);
      return new HashMap<>();
    }

    final int limit = 1000;

    final List<Outlink> outlinks = getFilteredOutlinks(sourcePage, limit);

    // For all links in a page, if the link is closer to the top, it has the higher score
    List<ScoreDatum> scoreData = IntStream.range(0, outlinks.size())
        .mapToObj(i -> new ScoreDatum(outlinks.size() - i, outlinks.get(i), depth))
        .collect(Collectors.toList());

    try {
      // In OPIC, the source page's score is distributed for all inlink/outlink pages
      scoringFilters.distributeScoreToOutlinks(sourceUrl, sourcePage, scoreData, outlinks.size());
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL : " + sourceUrl + " Exception:" + StringUtil.stringifyException(e));
    }

    counter.increase(NutchCounter.Counter.outlinks, scoreData.size());

    return scoreData.stream().map(d -> createNewDatum(d.getUrl(), d.getScore(), sourcePage))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }
}
