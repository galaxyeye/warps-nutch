package org.apache.nutch.indexer.metadata;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

/**
 * Created by vincent on 16-9-14.
 *
 * A simple regex extractor
 *
 * TODO : make it plugable
 */
public class SiteNames implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(SiteNames.class);

  private Configuration conf;
  private Map<String, String> siteNames = new HashedMap();

  public SiteNames() {

  }

  public SiteNames(Configuration conf) {
    setConf(conf);
  }

  public String getSiteName(String domain) {
    return siteNames.get(domain);
  }

  public int count() { return siteNames.size(); }

  public String dumpRules() {
    return StringUtils.join(siteNames, ", ");
  }

  /**
   * Sets the configuration.
   * */
  public void setConf(Configuration conf) {
    this.conf = conf;

    String file = conf.get("indxer.site.names.file");
    String stringRules = conf.get("indxer.site.names.rules");
    Reader reader;
    if (stringRules != null) { // takes precedence over files
      reader = new StringReader(stringRules);
    } else {
      reader = conf.getConfResourceAsReader(file);
    }
    try {
      if (reader == null) {
        reader = new FileReader(file);
      }

      readConfiguration(reader);
    } catch (IOException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  private void readConfiguration(Reader configReader) throws IOException {
    // Read the configuration file, line by line
    BufferedReader reader = new BufferedReader(configReader);
    String line;
    while ((line = reader.readLine()) != null) {
      if (StringUtils.isBlank(line) || line.startsWith("#")) {
        continue;
      }

      // add non-blank lines and non-commented lines
      String[] parts = line.split("\\s+");
      if (parts.length >= 2) {
        siteNames.put(parts[0], parts[1]);
      }
    }
  }
}
