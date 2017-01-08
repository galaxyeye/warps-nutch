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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.common.ObjectCache;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.stream.Collectors;

/** Creates and caches {@link URLFilter} implementing plugins. */
public class URLFilters2 {

  public Logger LOG = URLFilter.LOG;

  public static final String URLFILTER_ORDER = "urlfilter.order";

  private ArrayList<URLFilter> urlFilters;

  public URLFilters2(Configuration conf) {
    String order = conf.get(URLFILTER_ORDER);
    ObjectCache objectCache = ObjectCache.get(conf);
    urlFilters = objectCache.getObject(URLFilter.class.getName(), new ArrayList<URLFilter>());

    if (urlFilters == null) {
      String[] sequencedFilterNames = null;
      if (order != null && !order.trim().equals("")) {
        sequencedFilterNames = order.split("\\s+");
      }

      ArrayList<URLFilter> unorderedFilters = new ArrayList<>();
      ArrayList<URLFilter> orderedFilters = new ArrayList<>();

      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(URLFilter.X_POINT_ID);
        if (point == null) {
          throw new RuntimeException(URLFilter.X_POINT_ID + " not found.");
        }

        for (Extension extension : point.getExtensions()) {
          unorderedFilters.add((URLFilter) extension.getExtensionInstance());
        }

        // TODO : move to config file
        RegexURLFilter regexUrlFilter = new RegexURLFilter();
        regexUrlFilter.setConf(conf);
        unorderedFilters.add(regexUrlFilter);

        if (sequencedFilterNames == null) {
          objectCache.setObject(URLFilter.class.getName(), unorderedFilters);
        } else {
          for (String filterName : sequencedFilterNames) {
            URLFilter urlFilter = CollectionUtils.find(unorderedFilters, f -> f.getClass().getSimpleName().equals(filterName));
            if (urlFilter != null) {
              orderedFilters.add(urlFilter);
            }
          }
          objectCache.setObject(URLFilter.class.getName(), orderedFilters);
        }
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }

      urlFilters = objectCache.getObject(URLFilter.class.getName(), new ArrayList<URLFilter>());

      LOG.info("Active urls filters : " + toString());
    }
  }

  /** Run all defined urlFilters. Assume logical AND. */
  public String filter(String urlString) throws URLFilterException {
    for (URLFilter urlFilter : urlFilters) {
      if (urlString == null) {
        return null;
      }
      urlString = urlFilter.filter(urlString);
    }
    return urlString;
  }

  @Override
  public String toString() {
    return urlFilters.stream().map(f -> f.getClass().getSimpleName()).collect(Collectors.joining(", "));
  }
}
