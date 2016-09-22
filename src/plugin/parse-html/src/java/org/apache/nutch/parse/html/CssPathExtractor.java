package org.apache.nutch.parse.html;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by vincent on 16-9-14.
 *
 * A simple regex extractor
 *
 * TODO : make it plugable
 */
public class CssPathExtractor implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(CssPathExtractor.class);

  private Configuration conf;
  private Set<Rule> ruleSet = new LinkedHashSet<>();

  private void readConfiguration(Reader configReader) throws IOException {
    // Read the configuration file, line by line
    BufferedReader reader = new BufferedReader(configReader);
    String line;
    while ((line = reader.readLine()) != null) {
      String urlRegex = ".*";
      if (line.startsWith("[") && line.endsWith("]")) {
        urlRegex = StringUtils.substringBetween(":", "]");
      }

      if (StringUtils.isBlank(line) || line.startsWith("#")) {
        continue;
      }

      /**
       * Convert the first line into the second line
       * director  ".article > .director"
       * directory	.article > .director
       * The above line contains a tab
       * */
      line = line.replaceAll("\\s+\"", "\t");
      line = line.replaceAll("\"\\s+", "\t");
      line = line.replaceAll("\t\t", "\t");
      // add non-blank lines and non-commented lines
      String[] parts = line.split("\t");
      if (parts.length >= 2) {
        Rule rule = new Rule();

        rule.urlPattern = Pattern.compile(urlRegex);
        rule.name = parts[0];
        rule.csspath = Pattern.compile(parts[1]);

        for (int i = 2; i < parts.length; ++i) {
          String k = StringUtils.substringBefore(parts[i], "=");
          String v = StringUtils.substringAfter(parts[i], "=");
          rule.metadata.put(k, v);
        }

        ruleSet.add(rule);
      }
    }
  }

  private static class Rule {
    String name;
    Pattern urlPattern;
    Pattern csspath;
    Map<String, String> metadata = Maps.newHashMap();
  }

  public Map<String, String> extract(String url, String text) {
    Map<String, String> kvs = Maps.newHashMap();

    // TODO : Use css path

    return kvs;
  }

  /**
   * Sets the configuration.
   **/
  public void setConf(Configuration conf) {
    this.conf = conf;

    // domain file and attribute "file" take precedence if defined
    String file = conf.get("extractor.csspath.rule.file");
    String stringRules = conf.get("extractor.csspath.rule.rules");
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

}
