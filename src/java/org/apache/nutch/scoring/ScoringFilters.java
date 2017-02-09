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

package org.apache.nutch.scoring;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.common.ObjectCache;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Creates and caches {@link ScoringFilter} implementing plugins.
 * 
 * @author Andrzej Bialecki
 */
public class ScoringFilters extends Configured implements ScoringFilter {

  public static final Logger LOG = LoggerFactory.getLogger(ScoringFilters.class);

  private ScoringFilter[] scoringFilters;

  public ScoringFilters(Configuration conf) {
    super(conf);

    ObjectCache objectCache = ObjectCache.get(conf);
    String order = conf.get("scoring.filter.order");
    this.scoringFilters = (ScoringFilter[]) objectCache.getObject(ScoringFilter.class.getName());

    if (this.scoringFilters == null) {
      String[] orderedFilters = null;
      if (order != null && !order.trim().equals("")) {
        orderedFilters = order.split("\\s+");
      }

      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(ScoringFilter.X_POINT_ID);
        if (point == null) {
          throw new RuntimeException(ScoringFilter.X_POINT_ID + " not found.");
        }

        Extension[] extensions = point.getExtensions();
        HashMap<String, ScoringFilter> filterMap = new HashMap<>();
        for (Extension extension : extensions) {
          ScoringFilter filter = (ScoringFilter) extension.getExtensionInstance();
          if (!filterMap.containsKey(filter.getClass().getName())) {
            filterMap.put(filter.getClass().getName(), filter);
          }
        }

        if (orderedFilters == null) {
          objectCache.setObject(ScoringFilter.class.getName(), filterMap.values().toArray(new ScoringFilter[0]));
        } else {
          ScoringFilter[] filters = new ScoringFilter[orderedFilters.length];
          for (int i = 0; i < orderedFilters.length; i++) {
            filters[i] = filterMap.get(orderedFilters[i]);
          }
          objectCache.setObject(ScoringFilter.class.getName(), filters);
        }
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }

      this.scoringFilters = (ScoringFilter[]) objectCache.getObject(ScoringFilter.class.getName());

      if (this.scoringFilters.length == 0) {
        LOG.warn("Failed to found any scoring filters");
      }
    }
  }

  /** Calculate a sort value for Generate. */
  @Override
  public float generatorSortValue(String url, WebPage row, float initSort) {
    for (ScoringFilter filter : scoringFilters) {
      try {
        initSort = filter.generatorSortValue(url, row, initSort);
      } catch (ScoringFilterException e) {
        LOG.error(StringUtil.stringifyException(e));
      }
    }
    return initSort;
  }

  /** Calculate a new initial score, used when adding newly discovered pages. */
  @Override
  public void initialScore(String url, WebPage row) {
    for (ScoringFilter filter : scoringFilters) {
      try {
        filter.initialScore(url, row);
      } catch (ScoringFilterException e) {
        LOG.error(StringUtil.stringifyException(e));
      }
    }
  }

  /** Calculate a new initial score, used when injecting new pages. */
  @Override
  public void injectedScore(String url, WebPage row) {
    for (ScoringFilter filter : scoringFilters) {
      try {
        filter.injectedScore(url, row);
      } catch (ScoringFilterException e) {
        LOG.error(StringUtil.stringifyException(e));
      }
    }
  }

  @Override
  public void distributeScoreToOutlinks(
      String fromUrl, WebPage row, WebGraph graph, Collection<WebEdge> outLinkEdges, int allCount) {
    for (ScoringFilter filter : scoringFilters) {
      try {
        filter.distributeScoreToOutlinks(fromUrl, row, graph, outLinkEdges, allCount);
      } catch (ScoringFilterException e) {
        LOG.error(StringUtil.stringifyException(e));
      }
    }
  }

  @Override
  public void updateScore(String url, WebPage row, WebGraph graph, Collection<WebEdge> inLinkEdges) {
    for (ScoringFilter filter : scoringFilters) {
      try {
        filter.updateScore(url, row, graph, inLinkEdges);
      } catch (ScoringFilterException e) {
        LOG.error(StringUtil.stringifyException(e));
      }
    }
  }

  @Override
  public float indexerScore(String url, IndexDocument doc, WebPage row, float initScore) {
    for (ScoringFilter filter : scoringFilters) {
      try {
        initScore = filter.indexerScore(url, doc, row, initScore);
      } catch (ScoringFilterException e) {
        LOG.error(StringUtil.stringifyException(e));
      }
    }
    return initScore;
  }

  @Override
  public Collection<GoraWebPage.Field> getFields() {
    Set<GoraWebPage.Field> fields = new HashSet<>();
    for (ScoringFilter filter : scoringFilters) {
      Collection<GoraWebPage.Field> pluginFields = filter.getFields();
      if (pluginFields != null) {
        fields.addAll(pluginFields);
      }
    }
    return fields;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Arrays.stream(this.scoringFilters).forEach(scroingFilter -> sb.append(scroingFilter.getClass().getSimpleName()).append(", "));
    return sb.toString();
  }
}
