package org.apache.nutch.dbupdate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.graph.*;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.io.WebPageWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.persist.WebPage;
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
  public Pair<GraphGroupKey, NutchWritable> createMainDatum(String reversedUrl, WebPage mainPage) {
    NutchWritable nutchWritable = new NutchWritable();
    nutchWritable.set(new WebPageWritable(conf, mainPage));
    return Pair.of(new GraphGroupKey(reversedUrl, Float.MAX_VALUE), nutchWritable);
  }

  /**
   * Generate new Pages from outlinks
   *
   * TODO : Write the result into hdfs directly to deduce memory consumption
   * */
  public Map<GraphGroupKey, NutchWritable> createRowsFromOutlink(String sourceUrl, WebPage sourcePage) {
    final int depth = sourcePage.getDepth();
    if (depth >= maxDistance) {
      return Collections.emptyMap();
    }

    WebGraph webGraph = new WebGraph();
    WebVertex v1 = new WebVertex(sourceUrl, "", sourcePage, depth);
//    v1.addMetadata(PUBLISH_TIME, page.getPublishTime());
//    v1.addMetadata(TMP_CHARS, page.sniffTextLength());

    /* A loop in the graph */
    webGraph.addVerticesAndEdge(v1, v1);

    // 1. Create scoreData
//    List<WebEdge> outlinkScoreData = sourcePage.getOutlinks().entrySet().stream()
//        .limit(maxOutlinks)
//        .map(e -> new WebEdge(0.0f, e.getKey().toString(), e.getValue().toString(), depth))
//        .collect(Collectors.toList());
    sourcePage.getOutlinks().entrySet().stream()
        .map(l -> new WebVertex(l.getKey().toString(), l.getValue().toString(), null, depth + 1))
        .forEach(v2 -> webGraph.addVerticesAndEdge(v1, v2));

    counter.increase(NutchCounter.Counter.outlinks, webGraph.edgesOf(v1).size());

    // 2. Distribute score to outlinks
    try {
      // In OPIC, the source page's score is distributed for all inlink/outlink pages
      scoringFilters.distributeScoreToOutlinks(sourceUrl, sourcePage, webGraph.edgesOf(v1), webGraph.edgesOf(v1).size());
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL : " + sourceUrl + " Exception:" + StringUtil.stringifyException(e));
    }

    // 3. Create all new rows from outlink, ready to write to storage
    return webGraph.edgesOf(v1).stream()
        .map(d -> createOutlinkDatum(sourceUrl, d))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Build map phrase datum
   * */
  @NotNull
  private Pair<GraphGroupKey, NutchWritable> createOutlinkDatum(String sourceUrl, WebEdge edge) {
    String reversedOutUrl = TableUtil.reverseUrlOrEmpty(edge.getTarget().getUrl());

    // TODO : why set to be the source url?
//    edge.setUrl(sourceUrl);

//    reversedOutUrl = TableUtil.reverseUrlOrEmpty(edge.getUrl());
//    edge.setUrl(sourceUrl);
//    GraphGroupKey urlWithScore = new GraphGroupKey();
//    urlWithScore.setReversedUrl(reversedOutUrl);
//    urlWithScore.setScore(edge.getScore());
//
//    NutchWritable nutchWritable = new NutchWritable();
//    nutchWritable.set(edge);

    return Pair.of(new GraphGroupKey(), new NutchWritable());
  }
}
