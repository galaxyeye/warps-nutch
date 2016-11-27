package org.apache.nutch.crawl;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.nutch.metadata.Metadata.META_IS_SEED;

/**
 * Created by vincent on 16-9-24.
 */
public class SeedBuilder {

  public static final Logger LOG = LoggerFactory.getLogger(SeedBuilder.class);

  // The shortest url
  public static final String ShortestValidUrl = "ftp://t.tt";

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
  private int customFetchIntervalSec = -1;
  /** Fetch interval in second */
  private int fetchIntervalSec;
  private float scoreInjected;
  private long currentTime;

  public SeedBuilder(Configuration conf) {
    scoreFilters = new ScoringFilters(conf);
    fetchIntervalSec = getFetchIntervalSec();
    scoreInjected = conf.getFloat("db.score.injected", Float.MAX_VALUE);
    currentTime = System.currentTimeMillis();
  }

  public Params getParams() {
    return new Params(
        "className", this.getClass().getSimpleName(),
        "fetchIntervalSec", fetchIntervalSec,
        "scoreInjected", scoreInjected,
        "injectTime", DateTimeUtil.format(currentTime)
    );
  }

  public WebPage buildWebPage(String urlLine) {
    WebPage row = WebPage.newBuilder().build();

      /* Ignore line that start with # */
    if (urlLine.length() < ShortestValidUrl.length() || urlLine.startsWith("#")) {
      return null;
    }

    String url = StringUtils.substringBefore(urlLine, "\t");

    Map<String, String> metadata = buildMetadata(urlLine);
    // Add metadata to page
    TableUtil.putAllMetadata(row, metadata);

    String reversedUrl = null;
    try {
      reversedUrl = TableUtil.reverseUrl(url);
      row.setFetchTime(currentTime);
      row.setFetchInterval(fetchIntervalSec);
    }
    catch (MalformedURLException e) {
      LOG.warn("Ignore illegal formatted url : " + url);
    }

    if (reversedUrl == null) {
      LOG.warn("Ignore illegal formatted url : " + url);
      return row;
    }

    // TODO : Check the difference between hbase.url and page.baseUrl
    row.setTmporaryVariable("url", url);
    row.setTmporaryVariable("reversedUrl", reversedUrl);

    if (customPageScore != -1f) {
      row.setScore(customPageScore);
    }
    else {
      row.setScore(scoreInjected);
    }

    try {
      scoreFilters.injectedScore(url, row);
    } catch (ScoringFilterException e) {
      LOG.warn("Cannot filter injected score for " + url + ", using default. (" + e.getMessage() + ")");
    }

    TableUtil.setDistance(row, 0);

    Mark.INJECT_MARK.putMark(row, Nutch.YES_UTF8);

    return row;
  }

  public int getFetchIntervalSec() {
    // Fetch fetchIntervalSec, the default value is 30 days
    // int fetchIntervalSec = conf.getInt("db.fetch.fetchIntervalSec.default", DateTimeUtil.MONTH);

    // For seeds, we re-fetch it every time we start the loop
    int fetchInterval = (int)Duration.ofMinutes(1).getSeconds();

    if (customFetchIntervalSec != -1) {
      fetchInterval = customFetchIntervalSec;
    }

    return fetchInterval;
  }

  private Map<String, String> buildMetadata(String seedUrl) {
    Map<String, String> metadata = new TreeMap<>();
    metadata.put(META_IS_SEED, Nutch.YES_STRING);

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
          customFetchIntervalSec = StringUtil.tryParseInt(value, fetchIntervalSec);
        }
        else {
          metadata.put(name, value);
        }
      } // for
    } // if

    return metadata;
  }
}
