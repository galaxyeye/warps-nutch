package org.apache.nutch.parse.html;

import com.google.common.collect.*;
import com.kohlschutter.boilerpipe.labels.DefaultLabels;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by vincent on 16-9-14.
 *
 * A simple regex extractor
 */
public class RegexExtractor implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(RegexExtractor.class);

  private Configuration conf;
  private Set<Rule> ruleSet = new LinkedHashSet<>();

  private ListMultimap<String, String> labeledFieldRules = LinkedListMultimap.create();
  private ListMultimap<String, String> regexFieldRules = LinkedListMultimap.create();
  private Set<String> terminatingBlocksContains = Sets.newTreeSet();
  private Set<String> terminatingBlocksStartsWith = Sets.newTreeSet();

  private static class Rule {
    String name;
    Pattern urlPattern;
    Pattern kvPattern;
    Map<String, String> metadata = Maps.newHashMap();

    @Override
    public String toString() {
      return name + ", " + kvPattern;
    }
  }

  public RegexExtractor() {
    init();
  }

  public RegexExtractor(Configuration conf) {
    init();

    setConf(conf);
  }

  /**
   * TODO : load from configuration files
   * */
  private void init() {
    labeledFieldRules.put("article_title", DefaultLabels.TITLE);

    regexFieldRules.put("author", "(记者：)\\s*([一-龥]+)");
    regexFieldRules.put("director", "(编辑：)\\s*([一-龥]+)");
    regexFieldRules.put("reference", "(来源：)\\s*([一-龥]+)");
    regexFieldRules.put("reference", "(转载自：)\\s*([一-龥]+)");
    regexFieldRules.put("keywords", "(关键词：)\\s*(.+)");
    regexFieldRules.put("keywords", "(标签：)\\s*(.+)");

    terminatingBlocksContains.addAll(Lists.newArrayList(
        "All Rights Reserved",
        "Copyright ©",
        "what you think...",
        "add your comment",
        "add comment",
        "reader views",
        "have your say",
        "reader comments",
        "网络传播视听节目许可证号"
    ));

    terminatingBlocksStartsWith.addAll(Lists.newArrayList(
        "comments",
        "© reuters",
        "please rate this",
        "post a comment",
        "参与讨论",
        "精彩评论",
        "网友跟贴",
        "登录发帖",
        "版权所有",
        "京ICP证",
        "网友跟贴"
    ));
  }

  public Map<String, String> extract(String url, String text) {
    Map<String, String> kvs = Maps.newHashMap();

    for (Rule rule : ruleSet) {
      Matcher urlMatcher = rule.urlPattern.matcher(url);
      if (urlMatcher.matches()) {
        Matcher valueMatcher = rule.kvPattern.matcher(text);

        if (valueMatcher.find()) {
          if (valueMatcher.groupCount() == 2) {
            kvs.put(rule.name, valueMatcher.group(2));
          }
        }
      }
    }

    return kvs;
  }

  public ListMultimap<String, String> getRegexFieldRules() {
    return this.regexFieldRules;
  }

  public ListMultimap<String, String> getLabeledFieldRules() {
    return this.labeledFieldRules;
  }

  public Set<String> getTerminatingBlocksContains() {
    return this.terminatingBlocksContains;
  }

  public Set<String> getTerminatingBlocksStartsWith() {
    return this.terminatingBlocksStartsWith;
  }

  public String dumpRules() {
    return StringUtils.join(ruleSet, ", ");
  }

  /**
   * Sets the configuration.
   */
  public void setConf(Configuration conf) {
    this.conf = conf;

    String file = conf.get("extractor.regex.rule.file");
    String stringRules = conf.get("extractor.regex.rules");
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
      String urlRegex = ".*";
      if (line.startsWith("[") && line.endsWith("]")) {
        urlRegex = StringUtils.substringBetween(":", "]");
      }

      if (StringUtils.isBlank(line) || line.startsWith("#")) {
        continue;
      }

      // add non-blank lines and non-commented lines
      String[] parts = line.split("\\s+");
      if (parts.length >= 2) {
        Rule rule = new Rule();

        rule.urlPattern = Pattern.compile(urlRegex);
        rule.name = parts[0];
        rule.kvPattern = Pattern.compile(parts[1]);

        for (int i = 2; i < parts.length; ++i) {
          String k = StringUtils.substringBefore(parts[i], "=");
          String v = StringUtils.substringAfter(parts[i], "=");
          rule.metadata.put(k, v);
        }

        ruleSet.add(rule);
      }
    }

    for (Rule rule : ruleSet) {
      regexFieldRules.put(rule.name, rule.kvPattern.pattern());
    }
  }
}
