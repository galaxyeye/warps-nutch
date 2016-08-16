package org.apache.nutch.indexer;

/**
 * Created by vincent on 16-8-1.
 */

import org.apache.avro.util.Utf8;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.mapreduce.NutchMapper;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;

import java.io.IOException;

/**
 * Created by vincent on 16-7-30.
 */
public class IndexingMapper extends NutchMapper<String, WebPage, String, IndexDocument> {

  public enum Counter { unmatchStatus, notUpdated, indexFailed };

  private DataStore<String, WebPage> storage;
  private Utf8 batchId;
  private int count = 0;
  private boolean reindex = false;
  private int limit = -1;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();
    getCounter().register(Counter.class);

    batchId = new Utf8(conf.get(Nutch.GENERATOR_BATCH_ID, Nutch.ALL_BATCH_ID_STR));
    reindex = conf.getBoolean(Nutch.ARG_REINDEX, false);
    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    limit = conf.getInt(Nutch.ARG_LIMIT, -1);

    try {
      storage = StorageUtils.createWebStore(conf, String.class, WebPage.class);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    LOG.info(StringUtil.formatParams(
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
    }

    IndexDocument doc = new IndexDocument.Builder(conf).build(key, page);
    if (doc == null) {
      getCounter().increase(Counter.indexFailed);
      // LOG.debug("Failed to get IndexDocument for " + TableUtil.unreverseUrl(key));
      return;
    }

    // LOG.debug("Indexing : " + TableUtil.unreverseUrl(key));

    if (!reindex) {
      Mark.INDEX_MARK.putMark(page, Mark.UPDATEDB_MARK.checkMark(page));      
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
