/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.filter;

// JDK imports
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.ConfigUtils;

/**
 * Filters URLs based on a file of regular expressions using the
 * {@link java.util.regex Java Regex implementation}.
 */
public class RegexURLFilter extends RegexURLFilterBase {

  public static final String URLFILTER_REGEX_FILE = "urlfilter.regex.file";
  public static final String URLFILTER_REGEX_RULES = "urlfilter.regex.rules";

  public RegexURLFilter() {
    super();
  }

  public RegexURLFilter(File file) throws IllegalArgumentException, IOException {
    super(file);
  }

  public RegexURLFilter(String rules) throws IllegalArgumentException, IOException {
    super(rules);
  }

  RegexURLFilter(Reader reader) throws IOException, IllegalArgumentException {
    super(reader);
  }

  /*
   * ----------------------------------- * <implementation:RegexURLFilterBase> *
   * -----------------------------------
   */

  /**
   * Rules specified as a config property will override rules specified as a
   * config file.
   */
  protected Reader getRulesReader(Configuration conf) throws IOException {
    String stringRules = conf.get(URLFILTER_REGEX_RULES);
    if (stringRules != null) {
      if (LOG.isDebugEnabled()) {
        // LOG.debug("Url filter regex rules : \n" + stringRules);
      }

      return new StringReader(stringRules);
    }

    String fileRules = conf.get(URLFILTER_REGEX_FILE);
    return conf.getConfResourceAsReader(fileRules);
  }

  // Inherited Javadoc
  protected RegexRule createRule(boolean sign, String regex) {
    return new RegexRuleImpl(sign, regex);
  }

  private class RegexRuleImpl extends RegexRule {
    private Pattern pattern;

    RegexRuleImpl(boolean sign, String regex) {
      super(sign, regex);
      pattern = Pattern.compile(regex);
    }

    protected boolean match(String url) {
      if (url == null) {
        LOG.error("Null url passed into to RegexRuleFilter");
        return false;
      }

      return pattern.matcher(url).find();
    }
  }

  /*
   * ------------------------------------ * </implementation:RegexURLFilterBase>
   * * ------------------------------------
   */
  public static void main(String args[]) throws IOException {
    RegexURLFilter filter = new RegexURLFilter();
    Configuration conf = ConfigUtils.create();
    conf.set(URLFILTER_REGEX_RULES, "+^http://sh.lianjia.com/ershoufang/pg(.*)$\n+^http://sh.lianjia.com/ershoufang/SH(.+)/{0,1}$\n-.+\n ");
//    conf.set(URLFILTER_REGEX_RULES, "+.+");
    filter.setConf(conf);
    main(filter, args);
  }
}
