package org.apache.nutch.dbupdate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.WebPageWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.nutch.metadata.Nutch.PARAM_GENERATOR_MAX_DISTANCE;

/**
 * Created by vincent on 16-9-25.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class MapDatumBuilder {

  public static final Logger LOG = LoggerFactory.getLogger(MapDatumBuilder.class);

  private NutchCounter counter;
  private Configuration conf;

  private final int maxDistance;
  private final int maxOutlinks = 1000;
  private ScoringFilters scoringFilters;

  public MapDatumBuilder(NutchCounter counter, Configuration conf) {
    this.counter = counter;
    this.conf = conf;

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);

    maxDistance = conf.getInt(PARAM_GENERATOR_MAX_DISTANCE, -1);
    scoringFilters = new ScoringFilters(conf);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "maxDistance", maxDistance,
        "maxOutlinks", maxOutlinks
    ));
  }

  public void reset() { }

  /**
   * Build map phrase datum
   * */
  public Pair<UrlWithScore, NutchWritable> createMainDatum(String reversedUrl, WebPage mainPage) {
    NutchWritable nutchWritable = new NutchWritable();
    nutchWritable.set(new WebPageWritable(conf, mainPage.get()));
    return Pair.of(new UrlWithScore(reversedUrl, Float.MAX_VALUE), nutchWritable);
  }

  /**
   * Generate new Pages from outlinks
   *
   * TODO : Write the result into hdfs directly to deduce memory consumption
   * */
  public Map<UrlWithScore, NutchWritable> createRowsFromOutlink(String sourceUrl, WebPage sourcePage) {
    final int depth = sourcePage.getDepth();
    if (depth >= maxDistance) {
      return Collections.emptyMap();
    }

    // 1. Create scoreData
    List<ScoreDatum> outlinkScoreData = sourcePage.getOutlinks().entrySet().stream()
        .limit(maxOutlinks)
        .map(e -> new ScoreDatum(0.0f, e.getKey().toString(), e.getValue().toString(), depth))
        .collect(Collectors.toList());
    counter.increase(NutchCounter.Counter.outlinks, outlinkScoreData.size());

    // 2. Distribute score to outlinks
    try {
      // In OPIC, the source page's score is distributed for all inlink/outlink pages
      scoringFilters.distributeScoreToOutlinks(sourceUrl, sourcePage, outlinkScoreData, outlinkScoreData.size());
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL : " + sourceUrl + " Exception:" + StringUtil.stringifyException(e));
    }

    // 3. Create all new rows from outlink, ready to write to storage
    return outlinkScoreData.stream()
        .map(d -> createOutlinkDatum(sourceUrl, d))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Build map phrase datum
   * */
  @NotNull
  private Pair<UrlWithScore, NutchWritable> createOutlinkDatum(String sourceUrl, ScoreDatum scoreDatum) {
    String reversedOutUrl = TableUtil.reverseUrlOrEmpty(scoreDatum.getUrl());

    // TODO : why set to be the source url?
    scoreDatum.setUrl(sourceUrl);

//    reversedOutUrl = TableUtil.reverseUrlOrEmpty(scoreDatum.getUrl());
//    scoreDatum.setUrl(sourceUrl);
//    UrlWithScore urlWithScore = new UrlWithScore();
//    urlWithScore.setReversedUrl(reversedOutUrl);
//    urlWithScore.setScore(scoreDatum.getScore());
//
//    NutchWritable nutchWritable = new NutchWritable();
//    nutchWritable.set(scoreDatum);

    return Pair.of(new UrlWithScore(reversedOutUrl, scoreDatum.getScore()), new NutchWritable(scoreDatum));
  }
}
