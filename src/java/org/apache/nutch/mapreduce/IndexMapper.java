package org.apache.nutch.mapreduce;

import org.apache.avro.util.Utf8;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.gora.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TableUtil;

import java.io.IOException;
import java.time.Instant;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Nutch.*;

/**
 * Created by vincent on 16-7-30.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class IndexMapper extends NutchMapper<String, GoraWebPage, String, IndexDocument> {

  public enum Counter { unmatchStatus, notUpdated, alreadyIndexed, pageTruncated, parseFailed, indexFailed, shortContent, hasPublishTime, hasAuthor };

  private DataStore<String, GoraWebPage> storage;
  private ParseUtil parseUtil;
  private boolean skipTruncated;

  private String batchId;
  private boolean reindex = false;
  private int limit = -1;
  private int minTextLenght;

  private int count = 0;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();
    getCounter().register(Counter.class);

    String crawlId = conf.get(PARAM_CRAWL_ID);
    batchId = conf.get(PARAM_BATCH_ID, ALL_BATCH_ID_STR);
    reindex = conf.getBoolean(PARAM_REINDEX, false);
    limit = conf.getInt(PARAM_LIMIT, -1);
    minTextLenght = conf.getInt("indexer.minimal.text.length", 200);

    parseUtil = reindex ? new ParseUtil(conf) : null;
    skipTruncated = conf.getBoolean(ParserJob.SKIP_TRUNCATED, true);

    try {
      storage = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "reindex", reindex,
        "limit", limit,
        "minTextLenght", minTextLenght
    ));
  }

  public static boolean isParseSuccess(ParseStatus status) {
    if (status == null) {
      return false;
    }
    return status.getMajorCode() == ParseStatusCodes.SUCCESS;
  }

  @Override
  public void map(String reverseUrl, GoraWebPage row, Context context) throws IOException, InterruptedException {
    WebPage page = WebPage.wrap(row);

    try {
      getCounter().increase(rows);

      if (limit > -1 && count > limit) {
        stop("hit limit " + limit + ", finish mapper");
        return;
      }

      String url = TableUtil.unreverseUrl(reverseUrl);

      // For test only
      ParseStatus pstatus = page.getParseStatus();
      if (pstatus == null || !isParseSuccess(pstatus) || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        getCounter().increase(Counter.unmatchStatus);
        return; // filter urls not parsed
      }
      // End for test only

      if (!reindex) {
        // ParseStatus pstatus = page.getParseStatus();
        if (pstatus == null || !isParseSuccess(pstatus) || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
          getCounter().increase(Counter.unmatchStatus);
          return; // filter urls not parsed
        }

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

      if (reindex) {
        if (ParserMapper.isTruncated(url, page)) {
          getCounter().increase(Counter.pageTruncated);
          if (skipTruncated) {
            return;
          }
        }

        Parse parse = parseUtil.parse(url, page);
        if (parse == null) {
          getCounter().increase(Counter.parseFailed);
          return;
        }
      }

      IndexDocument doc = new IndexDocument.Builder(conf).build(reverseUrl, page);
      if (doc == null) {
        getCounter().increase(Counter.indexFailed);
        // LOG.debug("Failed to get IndexDocument for " + TableUtil.unreverseUrl(key));
        return;
      }

      String textContent = doc.getFieldValueAsString(DOC_FIELD_TEXT_CONTENT);
      if (textContent == null || textContent.length() < minTextLenght) {
        getCounter().increase(Counter.shortContent);
        return;
      }

      if (doc.getField("author") != null || doc.getField("director") != null) {
        getCounter().increase(Counter.hasAuthor);
      }

      if (doc.getField("publish_time") != null) {
        getCounter().increase(Counter.hasPublishTime);
      }

      // LOG.debug("Indexing : " + TableUtil.unreverseUrl(key));

      if (!reindex) {
        // Mark.INDEX_MARK.putMark(page, Mark.UPDATEDB_MARK.checkMark(page));
        Mark.INDEX_MARK.putMark(page, new Utf8(batchId));
      }

      // LOG.debug(TableUtil.toString(page.getText()));

      // Multiple output
      page.putIndexTimeHistory(Instant.now());

      storage.put(reverseUrl, page.get());
      context.write(reverseUrl, doc);

      getCounter().updateAffectedRows(doc.getUrl());

      ++count;
    }
    catch (Throwable e) {
      LOG.error(e.toString());
    }
  }

  @Override
  protected void cleanup(Context context) {
    super.cleanup(context);
    storage.close();
    LOG.info(getCounter().getStatusString());
  };
}
