package org.apache.nutch.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.filter.PageCategory;
import org.apache.nutch.mapreduce.NutchReporter;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * TODO : use better metrics module, for example, metrics from alibaba http://metrics.dropwizard.io
 * */

/**
 * Created by vincent on 16-10-12.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class NutchMetrics implements AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(NutchMetrics.class);
  public static final Logger REPORT_LOG = NutchReporter.chooseLog(false);

  private static NutchMetrics instance;

  private final Configuration conf;
  private Path reportDir;
  private Path unreachableHostsPath;
  private Map<Path, BufferedWriter> writers = new HashMap<>();

  /**
   * TODO : check if there is a singleton bug
   * */
  public static NutchMetrics getInstance(Configuration conf) {
    if(instance == null) {
      instance = new NutchMetrics(conf);
    }

    return instance;
  }

  private NutchMetrics(Configuration conf) {
    this.conf = conf;

    try {
      reportDir = ConfigUtils.getPath(conf, PARAM_NUTCH_REPORT_DIR, Paths.get(PATH_NUTCH_REPORT_DIR));
      reportDir = Paths.get(reportDir.toAbsolutePath().toString(), DateTimeUtil.format(System.currentTimeMillis(), "yyyyMMdd"));
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

  @Override
  public void close() throws Exception {
    writers.values().forEach(writer -> {
      try {
        writer.flush();
        writer.close();
      } catch (IOException e) {
        LOG.error(e.toString());
      }
    });
  }

  public Path getUnreachableHostsPath() { return unreachableHostsPath; }

  public void loadUnreachableHosts(Set<String> unreachableHosts) {
    try {
      Files.readAllLines(unreachableHostsPath).forEach(unreachableHosts::add);
    } catch (IOException e) {
      LOG.error(e.toString());
    }
  }

  public void reportRedirects(String redirectString, String reportSuffix) {
    // writeReport(redirectString, "fetch-redirects-" + reportSuffix + ".txt");
  }

  public void reportPageFromSeedersist(String report, String reportSuffix) {
    // String reportString = seedUrl + " -> " + url + "\n";
    writeReport(report + "\n", "fetch-urls-from-seed-persist-" + reportSuffix + ".txt");
  }

  public void reportPageFromSeed(String report, String reportSuffix) {
    // String reportString = seedUrl + " -> " + url + "\n";
    writeReport(report + "\n", "fetch-urls-from-seed-" + reportSuffix + ".txt");
  }

  public void reportFetchTimeHistory(String fetchTimeHistory, String reportSuffix) {
    // writeReport(fetchTimeHistory, "fetch-time-history-" + reportSuffix + ".txt");
  }

  public void reportGeneratedHosts(Set<String> hostNames, String postfix) {
    String report = "# Total " + hostNames.size() + " hosts generated : \n"
        + hostNames.stream().map(TableUtil::reverseHost).sorted().map(TableUtil::unreverseHost)
        .map(host -> String.format("%40s", host))
        .collect(Collectors.joining("\n"));

    writeReport(report, "generate-hosts-" + postfix + ".txt", true);
  }

  public void debugFetchLaterSeeds(String report, String reportSuffix) {
    writeReport(report + "\n", "seeds-fetch-later-" + reportSuffix + ".txt");
  }

  public void debugDepthUpdated(String report, String reportSuffix) {
    writeReport(report + "\n", "depth-updated-" + reportSuffix + ".txt");
  }

  public void debugUrls(String report, PageCategory pageCategory, String reportSuffix) {
    writeReport(report + "\n", "urls-" + pageCategory.name().toLowerCase() + "-" + reportSuffix + ".txt");
  }

  public void debugLongUrls(String report, String reportSuffix) {
    writeReport(report + "\n", "urls-long-" + reportSuffix + ".txt");
  }

  public void debugIndexDocTime(String timeStrings, String reportSuffix) {
    writeReport(timeStrings + "\n", "index-doc-time-" + reportSuffix + ".txt");
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

  public synchronized void writeReport(Path reportFile, String report, boolean printPath, boolean buffered) {
    try {
      if (!Files.exists(reportFile)) {
        Files.createDirectories(reportFile.getParent());
        Files.createFile(reportFile);
      }

      BufferedWriter writer = writers.get(reportFile);
      if (writer == null) {
        writer = Files.newBufferedWriter(reportFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        writers.put(reportFile, writer);
      }

      writer.write(report);

      // TODO : flush only when the buffer is full?
      writer.flush();
    } catch (IOException e) {
      LOG.error("Failed to write report : " + e.toString());
    }

    if (printPath) {
      LOG.info("Report written to " + reportFile.toAbsolutePath());
    }
  }

  public void writeReport(String report, String fileSuffix, boolean printPath) {
    Path reportFile = Paths.get(reportDir.toAbsolutePath().toString(), fileSuffix);
    writeReport(reportFile, report, printPath);
  }

}
