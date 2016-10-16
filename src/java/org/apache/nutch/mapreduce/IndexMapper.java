package org.apache.nutch.mapreduce;

/**
 * Created by vincent on 16-8-1.
 */

import org.apache.avro.util.Utf8;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Params;

import java.io.IOException;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * Created by vincent on 16-7-30.
 */
public class IndexMapper extends NutchMapper<String, WebPage, String, IndexDocument> {

  public enum Counter { unmatchStatus, notUpdated, alreadyIndexed, indexFailed };

  private DataStore<String, WebPage> storage;
  private String batchId;
  private int count = 0;
  private boolean reindex = false;
  private int limit = -1;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();
    getCounter().register(Counter.class);

    String crawlId = conf.get(PARAM_CRAWL_ID);
    batchId = conf.get(PARAM_BATCH_ID, ALL_BATCH_ID_STR);
    reindex = conf.getBoolean(ARG_REINDEX, false);
    limit = conf.getInt(ARG_LIMIT, -1);

    try {
      storage = StorageUtils.createWebStore(conf, String.class, WebPage.class);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "reindex", reindex,
        "limit", limit
    ));
  }

  public static boolean isParseSuccess(ParseStatus status) {
    if (status == null) {
      return false;
    }
    return status.getMajorCode() == ParseStatusCodes.SUCCESS;
  }

  @Override
  public void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
    if (limit > -1 && count > limit) {
      stop("hit limit " + limit + ", finish mapper.");
      return;
    }

    ParseStatus pstatus = page.getParseStatus();

    if (pstatus == null || !isParseSuccess(pstatus) || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
      getCounter().increase(Counter.unmatchStatus);
      return; // filter urls not parsed
    }

    if (!reindex) {
      Utf8 mark = Mark.UPDATEDB_MARK.checkMark(page);
      if (mark == null) {
        getCounter().increase(Counter.notUpdated);
        // LOG.debug("Not db updated : " + TableUtil.unreverseUrl(key));
        return;
      }

      mark = Mark.INDEX_MARK.checkMark(page);
      if (mark != null) {
        getCounter().increase(Counter.alreadyIndexed);
        // LOG.debug("Not db updated : " + TableUtil.unreverseUrl(key));
        return;
      }
    }

    IndexDocument doc = new IndexDocument.Builder(conf).build(key, page);
    if (doc == null) {
      getCounter().increase(Counter.indexFailed);
      // LOG.debug("Failed to get IndexDocument for " + TableUtil.unreverseUrl(key));
      return;
    }

    // LOG.debug("Indexing : " + TableUtil.unreverseUrl(key));

    if (!reindex) {
      // Mark.INDEX_MARK.putMark(page, Mark.UPDATEDB_MARK.checkMark(page));
      Mark.INDEX_MARK.putMark(page, new Utf8(batchId));
    }

    // LOG.debug(TableUtil.toString(page.getText()));

    // Multiple output
    storage.put(key, page);
    context.write(key, doc);

    getCounter().updateAffectedRows(doc.getUrl());
    
    ++count;
  }

  @Override
  protected void cleanup(Context context) {
    super.cleanup(context);
    storage.close();
    LOG.info(getCounter().getStatusString());
  };
}
