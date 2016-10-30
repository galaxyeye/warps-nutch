package org.apache.nutch.dbupdate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.WebPageWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.ALL_BATCH_ID_STR;
import static org.apache.nutch.metadata.Nutch.PARAM_BATCH_ID;

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

  private ScoringFilters scoringFilters;

  /**
   * Reuse variables for efficiency
   * */
  private final List<ScoreDatum> scoreData = new ArrayList<>(200);
  // There are no more than 200 outlinks in a page generally
  private final List<Pair<UrlWithScore, WebPageWritable>> newRows = new ArrayList<>(200);

  private final String batchId;

  private final boolean normalize;
  private final boolean filter;
  private final URLNormalizers normalizers;
  private final URLFilters urlFilters;
  private final CrawlFilters crawlFilters;
  private final boolean pageRankEnabled = false;

  public MapDatumBuilder(NutchCounter counter, Configuration conf) {
    this.counter = counter;
    this.conf = conf;

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);

    scoringFilters = new ScoringFilters(conf);
    batchId = conf.get(PARAM_BATCH_ID, ALL_BATCH_ID_STR);
    normalize = conf.getBoolean(URL_NORMALIZING, true);
    filter = conf.getBoolean(URL_FILTERING, true);

    normalizers = normalize ? new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK) : null;
    urlFilters = filter ? new URLFilters(conf) : null;

    crawlFilters = CrawlFilters.create(conf);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "normalize", normalize,
        "filter", filter,
        "pageRankEnabled", pageRankEnabled
    ));
  }

  public String filterUrl(WebPage page, String url) {
    try {
      if (normalize) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_OUTLINK);
      }

      // We always follow detail urls from seed page
      if (TableUtil.isSeed(page) && crawlFilters.isDetailUrl(url)) {
        return url;
      }

      if (filter) {
        url = urlFilters.filter(url);
      }

      if (crawlFilters.hasOldUrlDate(url)) {
        url = null;
      }

      if (crawlFilters.isSearchUrl(url) || crawlFilters.isMediaUrl(url)) {
        url = null;
      }
    } catch (URLFilterException|MalformedURLException e) {
      LOG.error(e.toString());
    }

    return url;
  }

  public Pair<UrlWithScore, WebPageWritable> buildDatum(String url, String reversedUrl, WebPage page) {
    int priority = TableUtil.calculatePriority(url, page, crawlFilters);
    float score = calculatePageScore(url, priority, page);

    WebPageWritable pageWritable = new WebPageWritable(conf, page);
    return Pair.of(new UrlWithScore(reversedUrl, score), pageWritable);
  }

  /**
   * Generate new Pages from outlinks
   *
   * TODO : Write the result into hdfs directly to deduce memory consumption
   * */
  public List<Pair<UrlWithScore, WebPageWritable>> buildNewRows(String url, WebPage page) {
    newRows.clear();
    scoreData.clear();

    // Do not dive too deep
    final int depth = getWebDepth(page);
    final int maxWebDepth = 3;
    if (depth > maxWebDepth) {
      counter.increase(NutchCounter.Counter.tooDeepPages);
      return newRows;
    }

    Map<CharSequence, CharSequence> outlinks = page.getOutlinks();
    if (outlinks == null || outlinks.isEmpty()) {
      return newRows;
    }

    outlinks.entrySet().stream().filter(e -> null != filterUrl(page, e.getKey().toString()))
        .map(e -> createScoreDatum(e.getKey(), e.getValue(), depth)).forEach(scoreData::add);

    // TODO : Outlink filtering (i.e. "only keep the first n outlinks")
    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData, outlinks.size());
    } catch (ScoringFilterException e) {
      counter.increase(NutchCounter.Counter.scoringErrors);
      LOG.warn("Distributing score failed for URL: " + url + " exception:" + StringUtil.stringifyException(e));
    }

    /**
     * annotation : @vincent the code is used to calculate page scores for each outlink of this page
     * in our case, we are not interested in the weight of each page,
     * we need the crawl goes faster, so we disable page rank by default
     * */
    int outlinkCount = 0;
    for (ScoreDatum scoreDatum : scoreData) {
      ++outlinkCount;

      try {
        String reversedUrl = TableUtil.reverseUrl(scoreDatum.getUrl());
        newRows.add(buildDatum(url, reversedUrl, page));
      } catch (MalformedURLException e) {
        counter.increase(NutchCounter.Counter.errors);
        LOG.warn(e.toString());
      }
    }

    counter.increase(NutchCounter.Counter.outlinks, outlinkCount);

    return newRows;
  }

  private int getWebDepth(WebPage page) {
    int depth = Integer.MAX_VALUE;
    CharSequence depthUtf8 = page.getMarkers().get(Nutch.DISTANCE);

    if (depthUtf8 != null) {
      depth = Integer.parseInt(depthUtf8.toString());
    }

    return depth;
  }

  private ScoreDatum createScoreDatum(CharSequence url, CharSequence anchor, int depth) {
    return new ScoreDatum(0.0f, url.toString(), anchor.toString(), depth);
  }

  /**
   * TODO : We calculate page score mainly in reduer
   * */
  private float calculatePageScore(String url, int priority, WebPage page) {
    return priority * 1.0f;
  }
}
