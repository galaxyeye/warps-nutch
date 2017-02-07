/*
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
package org.apache.nutch.filter.urlfilter.domain;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.filter.URLFilter;
import org.apache.nutch.net.domain.DomainSuffix;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * Filters URLs based on a file containing domain suffixes, domain names, and
 * hostnames. Only a url that matches one of the suffixes, domains, or hosts
 * present in the file is allowed.
 * </p>
 * 
 * <p>
 * Urls are checked in order of domain suffix, domain name, and hostname against
 * entries in the domain file. The domain file would be setup as follows with
 * one entry per line:
 * 
 * <pre>
 * com apache.org www.apache.org
 * </pre>
 * 
 * <p>
 * The first line is an example of a filter that would allow all .com domains.
 * The second line allows all urls from apache.org and all of its subdomains
 * such as lucene.apache.org and hadoop.apache.org. The third line would allow
 * only urls from www.apache.org. There is no specific ordering to entries. The
 * entries are from more general to more specific with the more general
 * overridding the more specific.
 * </p>
 * 
 * The domain file defaults to domain-urlfilter.txt in the classpath but can be
 * overridden using the:
 * 
 * <ul>
 * <ol>
 * property "urlfilter.domain.file" in ./conf/nutch-*.xml, and
 * </ol>
 * <ol>
 * attribute "file" in plugin.xml of this plugin
 * </ol>
 * </ul>
 * 
 * the attribute "file" has higher precedence if defined.
 */
public class DomainURLFilter implements URLFilter {

  private static final Logger LOG = LoggerFactory.getLogger(DomainURLFilter.class);

  // read in attribute "file" of this plugin.
  private static String PLUGIN_NAME = "urlfilter-domain";
  private Configuration conf;
  private String domainFile = null;
  private String attributeFile = null;
  private Set<String> domainSet = new LinkedHashSet<>();

  /**
   * Default constructor.
   */
  public DomainURLFilter() {
  }

  /**
   * Constructor that specifies the domain file to use.
   * 
   * @param domainFile
   *          The domain file, overrides domain-urlfilter.text default.
   */
  public DomainURLFilter(String domainFile) {
    this.domainFile = domainFile;
  }

  /**
   * Constructor that specifies the domain file to use.
   *
   * @param domainFile
   *          The domain file, overrides domain-urlfilter.text default.
   *
   * @param conf
   *          The configuration
   */
  public DomainURLFilter(String domainFile, Configuration conf) {
    this.domainFile = domainFile;
    setConf(conf);
  }

  public String filter(String url) {
    try {
      // match for suffix, domain, and host in that order. more general will
      // override more specific
      String domain = URLUtil.getDomainName(url).toLowerCase().trim();
      String host = URLUtil.getHostName(url);
      String suffix = null;
      DomainSuffix domainSuffix = URLUtil.getDomainSuffix(url);
      if (domainSuffix != null) {
        suffix = domainSuffix.getDomain();
      }

      if (domainSet.contains(suffix) || domainSet.contains(domain) || domainSet.contains(host)) {
        return url;
      }

      // doesn't match, don't allow
      return null;
    } catch (Exception e) {

      // if an error happens, allow the url to pass
      LOG.error("Could not apply filter on url: " + url + "\n" + StringUtil.stringifyException(e));
      return null;
    }
  }

  /**
   * Sets the configuration.
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    load();
    LOG.debug("domainSet : " + StringUtils.join(domainSet, ", "));
  }

  public Configuration getConf() { return this.conf; }

  private void load() {
    // get the extensions for domain urlfilter
    Extension[] extensions = PluginRepository.get(conf).getExtensionPoint(URLFilter.class.getName()).getExtensions();
    for (Extension extension : extensions) {
      // LOG.info(extension.getDescriptor().toString());
      if (extension.getDescriptor().getPluginId().equals(PLUGIN_NAME)) {
        attributeFile = extension.getAttribute("file");
        break;
      }
    }

    // handle blank non empty input
    if (attributeFile != null && attributeFile.trim().equals("")) {
      attributeFile = null;
    }

    if (attributeFile != null) {
      LOG.info("Attribute \"file\" is defined for plugin " + PLUGIN_NAME + " as " + attributeFile);
    } else {
      LOG.warn("Attribute \"file\" is not defined in plugin.xml for plugin " + PLUGIN_NAME);
    }

    try (Reader reader = getReader()) {
      domainSet = new BufferedReader(reader).lines()
          .map(String::toLowerCase)
          .filter(l -> !l.startsWith("#") && !l.trim().isEmpty())
          .collect(Collectors.toSet());
    } catch (IOException e) {
      LOG.error(StringUtil.stringifyException(e));
    }
  }

  private Reader getReader() throws FileNotFoundException {
    // domain file and attribute "file" take precedence if defined
    String file = conf.get("urlfilter.domain.file");
    String stringRules = conf.get("urlfilter.domain.rules");

    if (domainFile != null) {
      file = domainFile;
    } else if (attributeFile != null) {
      file = attributeFile;
    }

    Reader reader;
    if (stringRules != null) { // takes precedence over files
      reader = new StringReader(stringRules);
    }
    else {
      reader = conf.getConfResourceAsReader(file);
    }

    if (reader == null) {
      reader = new FileReader(file);
    }

    return reader;
  }

}
