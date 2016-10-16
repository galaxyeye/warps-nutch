package org.apache.nutch.indexer.metadata;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by vincent on 16-9-14.
 *
 * A simple regex extractor
 *
 * TODO : make it plugable
 */
public class ResourceCategory implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceCategory.class);

  private Configuration conf;
  private Map<String, String> resourceCategories = new LinkedHashMap<>();

  public ResourceCategory() {
  }

  public ResourceCategory(Configuration conf) {
    setConf(conf);
  }

  /**
   *
   * */
  public String getCategory(String url) {
    for (String regex : resourceCategories.keySet()) {
      if (url.matches(regex)) {
        return resourceCategories.get(regex);
      }
    }

    return "";
  }

  public int count() { return resourceCategories.size(); }

  public String dumpRules() {
    return StringUtils.join(resourceCategories, ", ");
  }

  /**
   * Sets the configuration.
   * */
  public void setConf(Configuration conf) {
    this.conf = conf;

    String file = conf.get("indxer.resource.category.file");
    String stringRules = conf.get("indxer.resource.category.rules");
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
        resourceCategories.put(parts[0], parts[1]);
      }
    }
  }
}
