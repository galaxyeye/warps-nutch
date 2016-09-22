package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.ObjectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Creates and caches {@link IndexingFilter} implementing plugins. */
public class IndexingFilters {

  public static final String INDEXINGFILTER_ORDER = "indexingfilter.order";

  public final static Logger LOG = LoggerFactory.getLogger(IndexingFilters.class);

  private IndexingFilter[] indexingFilters;

  public IndexingFilters(Configuration conf) {
    /* Get indexingfilter.order property */
    String order = conf.get(INDEXINGFILTER_ORDER);
    String[] orderedFilters = null;
    if (order != null && !order.trim().equals("")) {
      orderedFilters = order.split("\\s+");
    }

    ObjectCache objectCache = ObjectCache.get(conf);
    String cacheId = IndexingFilter.class.getName();

    this.indexingFilters = (IndexingFilter[]) objectCache.getObject(cacheId);
    if (this.indexingFilters == null) {
      /*
       * If ordered filters are required, prepare array of filters based on
       * property
       */
      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(IndexingFilter.X_POINT_ID);
        if (point == null) {
          throw new RuntimeException(IndexingFilter.X_POINT_ID + " not found.");
        }

        Extension[] extensions = point.getExtensions();
        HashMap<String, IndexingFilter> filterMap = new HashMap<>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          IndexingFilter filter = (IndexingFilter) extension.getExtensionInstance();

          String filterName = filter.getClass().getName();

          filterMap.put(filterName, filter);
        }

        /*
         * If no ordered filters required, just get the filters in an
         * indeterminate order
         */
        if (orderedFilters == null) {
          objectCache.setObject(cacheId, filterMap.values().toArray(new IndexingFilter[0]));
        } else {
          ArrayList<IndexingFilter> filters = new ArrayList<>();
          for (int i = 0; i < orderedFilters.length; i++) {
            IndexingFilter filter = filterMap.get(orderedFilters[i]);
            if (filter != null) {
              filters.add(filter);
            }
          }

          objectCache.setObject(cacheId, filters.toArray(new IndexingFilter[filters.size()]));
        }
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }

      this.indexingFilters = (IndexingFilter[]) objectCache.getObject(cacheId);

      LOG.info("Active indexing filters : " + toString());
    }
  }

  /** Run all defined filters. */
  public IndexDocument filter(IndexDocument doc, String url, WebPage page) throws IndexingException {
    for (IndexingFilter indexingFilter : indexingFilters) {
      doc = indexingFilter.filter(doc, url, page);
      // break the loop if an indexing filter discards the doc
      if (doc == null) {
        return null;
      }
    }

    return doc;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Arrays.stream(this.indexingFilters)
        .forEach(indexingFilter -> sb.append(indexingFilter.getClass().getSimpleName()).append(", "));
    return sb.toString();
  }

  /**
   * Gets all the fields for a given {@link WebPage} Many datastores need to
   * setup the mapreduce job by specifying the fields needed. All extensions
   * that work on HWebPage are able to specify what fields they need.
   */
  public Collection<WebPage.Field> getFields() {
    Collection<WebPage.Field> columns = new HashSet<>();
    for (IndexingFilter indexingFilter : indexingFilters) {
      Collection<WebPage.Field> fields = indexingFilter.getFields();
      if (fields != null) {
        columns.addAll(fields);
      }
    }
    return columns;
  }
}
