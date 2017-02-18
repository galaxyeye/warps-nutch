package org.apache.nutch.crawl;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.common.Parameterized;
import org.apache.nutch.common.Params;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.nutch.metadata.Nutch.SHORTEST_VALID_URL_LENGTH;

/**
 * Created by vincent on 16-9-24.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class SeedBuilder implements Parameterized {

  public static final Logger LOG = LoggerFactory.getLogger(SeedBuilder.class);

  /**
   * metadata key reserved for setting a custom fetchIntervalSec for a specific URL
   */
  public static final String NutchFetchIntervalMDName = "nutch.fetchIntervalSec";

  /** metadata key reserved for setting a custom score for a specific URL */
  public static final String NutchScoreMDName = "nutch.score";

  private ScoringFilters scoreFilters;
  /** Custom page score */
  private float customPageScore;
  /** Custom fetch interval in second */
  private Duration customFetchInterval = Duration.ofSeconds(-1);
  /** Fetch interval in second */
  private Duration fetchInterval;
  private float scoreInjected;
  private Instant currentTime;

  public SeedBuilder(Configuration conf) {
    scoreFilters = new ScoringFilters(conf);
    fetchInterval = getFetchInterval();
    scoreInjected = conf.getFloat("db.score.injected", Float.MAX_VALUE);
    currentTime = Instant.now();
  }

  @Override
  public Params getParams() {
    return Params.of(
        "className", this.getClass().getSimpleName(),
        "fetchInterval", fetchInterval,
        "scoreInjected", scoreInjected,
        "injectTime", DateTimeUtil.format(currentTime)
    );
  }

  public WebPage buildWebPage(String urlLine) {
      /* Ignore line that start with # */
    if (urlLine.length() < SHORTEST_VALID_URL_LENGTH || urlLine.startsWith("#")) {
      return null;
    }

    String url = StringUtils.substringBefore(urlLine, "\t");
    WebPage page = WebPage.newWebPage(url);

    if (!page.hasLegalUrl()) {
      LOG.warn("Ignore illegal formatted url : " + url);
      return null;
    }

    // Add metadata to page
    page.putAllMetadata(buildMetadata(urlLine));

    page.setFetchTime(currentTime);
    page.setFetchInterval(fetchInterval);

    if (customPageScore != -1f) {
      page.setScore(customPageScore);
    }
    else {
      page.setScore(scoreInjected);
    }

    scoreFilters.injectedScore(url, page);

    page.setFetchCount(0);
    page.setDistance(0);
    page.markAsSeed();

    page.putMark(Mark.INJECT, Nutch.YES_STRING);

    return page;
  }

  public Duration getFetchInterval() {
    // Crawl seed pages as soon as possible
    fetchInterval = Duration.ofMinutes(1);

    if (customFetchInterval.getSeconds() != -1) {
      fetchInterval = customFetchInterval;
    }

    return fetchInterval;
  }

  private Map<String, String> buildMetadata(String seedUrl) {
    Map<String, String> metadata = new TreeMap<>();

    // if tabs : metadata that could be stored
    // must be name=value and separated by \t
    if (seedUrl.contains("\t")) {
      String[] splits = seedUrl.split("\t");

      for (int s = 1; s < splits.length; s++) {
        // find separation between name and value
        int indexEquals = splits[s].indexOf("=");
        if (indexEquals == -1) {
          // skip anything without a =
          continue;
        }

        String name = splits[s].substring(0, indexEquals);
        String value = splits[s].substring(indexEquals + 1);
        if (name.equals(NutchScoreMDName)) {
          customPageScore = StringUtil.tryParseFloat(value, -1f);
        }
        else if (name.equals(NutchFetchIntervalMDName)) {
          long duration = StringUtil.tryParseLong(value, fetchInterval.getSeconds());
          customFetchInterval = Duration.ofSeconds(duration);
        }
        else {
          metadata.put(name, value);
        }
      } // for
    } // if

    return metadata;
  }
}
