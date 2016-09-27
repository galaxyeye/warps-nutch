package org.apache.nutch.dbupdate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.mapreduce.DbUpdateMapper;
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

  private ScoringFilters scoringFilters;

  /**
   * Reuse variables for efficiency
   * */
  private final List<ScoreDatum> scoreData = new ArrayList<>(200);
  // There are no more than 200 outlinks in a page generally
  private final List<Pair<UrlWithScore, WebPageWritable>> newRows = new ArrayList<>(200);

  private String batchId;

  private boolean normalize;
  private boolean filter;
  private URLNormalizers normalizers;
  private URLFilters urlFilters;
  private CrawlFilters crawlFilters;
  private boolean pageRankEnabled = false;

  public MapDatumBuilder(NutchCounter counter, Configuration conf) {
    this.counter = counter;
    this.conf = conf;

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);

    scoringFilters = new ScoringFilters(conf);
    batchId = conf.get(BATCH_NAME_KEY, ALL_BATCH_ID_STR);
    normalize = conf.getBoolean(URL_NORMALIZING, true);
    filter = conf.getBoolean(URL_FILTERING, true);

    if (normalize) {
      normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
    }

    if (filter) {
      urlFilters = new URLFilters(conf);
    }

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

  public String filter(String url) {
    try {
      if (normalize) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_OUTLINK);
      }

      if (filter) {
        url = urlFilters.filter(url);
      }
    } catch (URLFilterException|MalformedURLException e) {
      LOG.error(e.toString());
    }

    return url;
  }

  /**
   * Map sort by score,
   * */
  public Pair<UrlWithScore, WebPageWritable> buildDatum(String url, String reversedUrl, WebPage page) {
    float score = calculatePageScore(url, page);
    calculatePriority(url, page);
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

    Map<CharSequence, CharSequence> outlinks = page.getOutlinks();
    if (outlinks == null || outlinks.isEmpty()) {
      return newRows;
    }

    final int depth = getWebDepth(page);
    outlinks.entrySet().forEach(e -> scoreData.add(createScoreDatum(e.getKey(), e.getValue(), depth)));

    // TODO : Outlink filtering (i.e. "only keep the first n outlinks")
    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData, outlinks.size());
    } catch (ScoringFilterException e) {
      counter.increase(DbUpdateMapper.Counter.errors);
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
        newRows.add(buildDatum(reversedUrl, url, page));
      } catch (MalformedURLException e) {
        counter.increase(DbUpdateMapper.Counter.errors);
        LOG.warn(e.toString());
      }
    }

    counter.increase(DbUpdateMapper.Counter.outlinkCount, outlinkCount);

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
  private float calculatePageScore(String url, WebPage page) {
    return 0.0f;
  }

  /**
   * basically, if a page looks more like a detail page,
   * the page should be fetched with higher priority
   * @author vincent
   * */
  private void calculatePriority(String url, WebPage page) {
    if (crawlFilters.isDetailUrl(url)) {
      TableUtil.setPriorityIfAbsent(page, FETCH_PRIORITY_DETAIL_PAGE);
    }

    String pageCategory = StringUtil.sniffPageCategory(url);
    if (pageCategory.equalsIgnoreCase("detail")) {
      TableUtil.setPriorityIfAbsent(page, FETCH_PRIORITY_DETAIL_PAGE);
    }

    TableUtil.setPriorityIfAbsent(page, FETCH_PRIORITY_DEFAULT);
  }
}
