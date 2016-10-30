package org.apache.nutch.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.mapreduce.NutchReporter;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * TODO : use better metrics module, for example, http://metrics.dropwizard.io
 * */

/**
 * Created by vincent on 16-10-12.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class NutchMetrics {

  public static final Logger LOG = LoggerFactory.getLogger(NutchMetrics.class);
  public static final Logger REPORT_LOG = NutchReporter.chooseLog(false);

  private final Configuration conf;
  private Path reportDir;
  private Path unreachableHostsPath;

  public NutchMetrics(Configuration conf) {
    this.conf = conf;

    try {
      reportDir = NutchConfiguration.getPath(conf, PARAM_NUTCH_REPORT_DIR, Paths.get(PATH_NUTCH_REPORT_DIR));
      reportDir = Paths.get(reportDir.toAbsolutePath().toString(), TimingUtil.format(System.currentTimeMillis(), "yyyyMMdd"));
      Files.createDirectories(reportDir);

      unreachableHostsPath = Paths.get(reportDir.toAbsolutePath().toString(), FILE_UNREACHABLE_HOSTS);
      Files.createDirectories(unreachableHostsPath.getParent());

      if (!Files.exists(unreachableHostsPath)) {
        Files.createFile(unreachableHostsPath);
      }
    } catch (IOException e) {
      LOG.error(e.toString());
    }
  }

  public Path getUnreachableHostsPath() { return unreachableHostsPath; }

  public void loadUnreachableHosts(Set<String> unreachableHosts) {
    try {
      Files.readAllLines(unreachableHostsPath).stream().forEach(unreachableHosts::add);
    } catch (IOException e) {
      LOG.error(e.toString());
    }
  }

  public void reportRedirects(String redirectString, String reportSuffix) {
    writeReport(redirectString, "fetch-redirects-" + reportSuffix + ".txt");
  }

  public void reportFetchTimeHistory(String fetchTimeHistory, String reportSuffix) {
    writeReport(fetchTimeHistory, "fetch-time-history-" + reportSuffix + ".txt");
  }

  public void reportGeneratedHosts(Set<String> hostNames, String postfix) {
    String report = "# Total " + hostNames.size() + " hosts generated : \n"
        + hostNames.stream().map(TableUtil::reverseHost).sorted().map(TableUtil::unreverseHost)
        .map(host -> String.format("%40s", host))
        .collect(Collectors.joining("\n"));

    writeReport(report, "generate-hosts-" + postfix + ".txt", true);
  }

  public void debugIndexUrls(String indexUrl, String reportSuffix) {
    writeReport(indexUrl + "\n", "urls-index-" + reportSuffix + ".txt");
  }

  public void debugSearchUrls(String searchUrl, String reportSuffix) {
    writeReport(searchUrl + "\n", "urls-search-" + reportSuffix + ".txt");
  }

  public void debugDetailUrls(String detailUrl, String reportSuffix) {
    writeReport(detailUrl + "\n", "urls-detail-" + reportSuffix + ".txt");
  }

  public void debugLongUrls(String longUrl, String reportSuffix) {
    writeReport(longUrl + "\n", "urls-long-" + reportSuffix + ".txt");
  }

  public void debugMediaUrls(String longUrl, String reportSuffix) {
    writeReport(longUrl + "\n", "urls-media-" + reportSuffix + ".txt");
  }

  public void debugBBSUrls(String longUrl, String reportSuffix) {
    writeReport(longUrl + "\n", "urls-bbs-" + reportSuffix + ".txt");
  }

  public void debugBlogUrls(String longUrl, String reportSuffix) {
    writeReport(longUrl + "\n", "urls-blog-" + reportSuffix + ".txt");
  }

  public void debugTiebaUrls(String longUrl, String reportSuffix) {
    writeReport(longUrl + "\n", "urls-tieba-" + reportSuffix + ".txt");
  }

  public void debugUnknownTypeUrls(String longUrl, String reportSuffix) {
    writeReport(longUrl + "\n", "urls-unknown-" + reportSuffix + ".txt");
  }

  public void writeReport(Path reportFile, String report) {
    writeReport(reportFile, report, true);
  }

  public void writeReport(String report, String fileSuffix) {
    writeReport(report, fileSuffix, false);
  }

  public void writeReport(Path reportFile, String report, boolean printPath) {
    writeReport(reportFile, report, printPath, true);
  }

  public void writeReport(Path reportFile, String report, boolean printPath, boolean buffered) {
    try {
      if (!Files.exists(reportFile)) {
        Files.createDirectories(reportFile.getParent());
        Files.createFile(reportFile);
      }

      // TODO : flush only if the buffer is full or the program is about to exit
      BufferedWriter writer = Files.newBufferedWriter(reportFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
      writer.write(report);
      writer.flush();

      if (printPath) {
        Log.info("Report written to " + reportFile.toAbsolutePath());
      }
    } catch (IOException e) {
      Log.warn("Failed to write report : " + e.toString());
    }
  }

  public void writeReport(String report, String fileSuffix, boolean printPath) {
    Path reportFile = Paths.get(reportDir.toAbsolutePath().toString(), fileSuffix);
    writeReport(reportFile, report, printPath);
  }
}
