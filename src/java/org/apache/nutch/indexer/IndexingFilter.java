package org.apache.nutch.indexer;

/**
 * Created by vincent on 16-8-1.
 */
import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.plugin.FieldPluggable;
import org.apache.nutch.storage.WrappedWebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

// Nutch imports

/**
 * Extension point for indexing. Permits one to add metadata to the indexed
 * fields. All plugins found which implement this extension point are run
 * sequentially on the parse.
 */
public interface IndexingFilter extends FieldPluggable, Configurable {

  Logger LOG = LoggerFactory.getLogger(IndexingFilter.class);

  /** The name of the extension point. */
  String X_POINT_ID = IndexingFilter.class.getName();

  /**
   * Adds fields or otherwise modifies the document that will be indexed for a
   * parse. Unwanted documents can be removed from indexing by returning a null
   * value.
   *
   * @param doc
   *          document instance for collecting fields
   * @param url
   *          page url
   * @param page
   * @return modified (or a new) document instance, or null (meaning the
   *         document should be discarded)
   * @throws IndexingException
   */
  IndexDocument filter(IndexDocument doc, String url, WrappedWebPage page) throws IndexingException;

  Collection<GoraWebPage.Field> getFields();
}
