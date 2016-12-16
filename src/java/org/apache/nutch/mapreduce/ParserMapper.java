package org.apache.nutch.mapreduce;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WrappedWebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Nutch.*;

public class ParserMapper extends NutchMapper<String, GoraWebPage, String, GoraWebPage> {

  public static final Logger LOG = ParserJob.LOG;

  public enum Counter { notFetchedPages, alreadyParsedPages, truncatedPages, notParsed, parseSuccess, parseFailed }

  private ParseUtil parseUtil;
  private boolean resume;
  private boolean force;
  private boolean reparse;
  private Utf8 batchId;
  private int limit = -1;
  private boolean skipTruncated;

  private int count = 0;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();
    getCounter().register(Counter.class);

    batchId = new Utf8(conf.get(PARAM_BATCH_ID, ALL_BATCH_ID_STR));
    parseUtil = new ParseUtil(conf);
    resume = conf.getBoolean(PARAM_RESUME, false);
    reparse = conf.getBoolean(PARAM_REPARSE, false);
    force = conf.getBoolean(PARAM_FORCE, false);
    limit = conf.getInt(PARAM_LIMIT, -1);
    skipTruncated = conf.getBoolean(ParserJob.SKIP_TRUNCATED, true);

    LOG.info(Params.format(
        "batchId", batchId,
        "resume", resume,
        "reparse", reparse,
        "force", force,
        "limit", limit,
        "skipTruncated", skipTruncated
    ));
  }

  @Override
  public void map(String reverseUrl, GoraWebPage row, Context context) {
    WrappedWebPage page = WrappedWebPage.wrap(row);

    try {
      getCounter().increase(rows);

      if (limit > -1 && count > limit) {
        stop("hit limit " + limit + ", finish mapper.");
        return;
      }

      String url = TableUtil.unreverseUrl(reverseUrl);

      if (!shouldProcess(url, page)) {
        return;
      }

      Parse parse = parseUtil.process(url, page);
      if (parse == null) {
        getCounter().increase(Counter.parseFailed);
        return;
      }

      // if where is FETCH_MARK set, we also have PARSE_MARK set after the parse

      ParseStatus pstatus = page.getParseStatus();

      countParseStatus(pstatus);
      if (parse.getOutlinks() != null) {
        getCounter().increase(NutchCounter.Counter.outlinks, parse.getOutlinks().length);
      }
      getCounter().updateAffectedRows(url);

      context.write(reverseUrl, page.get());

      ++count;
    }
    catch (Throwable e) {
      LOG.error(e.toString());
    }
  }

  // 0 : "notparsed", 1 : "success", 2 : "failed"
  private void countParseStatus(ParseStatus pstatus) {
    if (pstatus == null) {
      return;
    }

    Counter counter = Counter.parseSuccess;

    switch (pstatus.getMajorCode()) {
      case 0 : counter = Counter.notParsed; break;
      case 1 : counter = Counter.parseSuccess; break;
      default : counter = Counter.parseFailed; break;
    }

    getCounter().increase(counter);
  }

  private boolean shouldProcess(String url, WrappedWebPage page) {
    if (!reparse && !Mark.FETCH_MARK.hasMark(page)) {
      getCounter().increase(Counter.notFetchedPages);

      if (LOG.isDebugEnabled()) {
//        LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; not fetched yet");
      }

      return false;
    }

    if (!reparse && resume && Mark.PARSE_MARK.hasMark(page)) {
      getCounter().increase(Counter.alreadyParsedPages);

      if (!force) {
        LOG.debug("Skipping " + url + "; already parsed");
        return false;
      }

      LOG.debug("Forced parsing " + url + "; already parsed");
    } // if resume

    if (skipTruncated && isTruncated(url, page)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Page truncated, ignore");
      }

      getCounter().increase(Counter.truncatedPages);

      return false;
    }

    return true;
  }

  /**
   * Checks if the page's content is truncated.
   * 
   * @param url
   * @param page
   * @return If the page is truncated <code>true</code>. When it is not, or when
   *         it could be determined, <code>false</code>.
   */
  public static boolean isTruncated(String url, WrappedWebPage page) {
    ByteBuffer content = page.getContent();
    if (content == null) {
      return false;
    }

    CharSequence lengthUtf8 = page.getHeaders().get(new Utf8(HttpHeaders.CONTENT_LENGTH));
    if (lengthUtf8 == null) {
      return false;
    }

    String lengthStr = lengthUtf8.toString().trim();
    if (StringUtil.isEmpty(lengthStr)) {
      return false;
    }

    int inHeaderSize;
    try {
      inHeaderSize = Integer.parseInt(lengthStr);
    } catch (NumberFormatException e) {
      LOG.warn("Wrong contentlength format for " + url, e);
      return false;
    }

    int actualSize = content.limit();
    if (inHeaderSize > actualSize) {
      // LOG.warn(url + " skipped. Content of size " + inHeaderSize + " was truncated to " + actualSize);
      return true;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(url + " actualSize=" + actualSize + " inHeaderSize=" + inHeaderSize);
    }

    return false;
  }
} // ParserMapper
