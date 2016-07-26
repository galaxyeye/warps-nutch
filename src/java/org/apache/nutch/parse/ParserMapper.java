package org.apache.nutch.parse;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.mapreduce.NutchMapper;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ParserMapper extends NutchMapper<String, WebPage, String, WebPage> {

  public static final Logger LOG = ParserJob.LOG;

  public enum Counter { notFetchedPages, alreadyParsedPages, truncatedPages, notParsed, parseSuccess, parseFailed };

  private ParseUtil parseUtil;
  private boolean resume;
  private boolean force;
  private boolean reparse;
  private Utf8 batchId;
  private boolean skipTruncated;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();
    getCounter().register(Counter.class);

    batchId = new Utf8(conf.get(Nutch.GENERATOR_BATCH_ID, Nutch.ALL_BATCH_ID_STR));
    parseUtil = new ParseUtil(conf);
    resume = conf.getBoolean(ParserJob.RESUME_KEY, false);
    reparse = batchId.equals(ParserJob.REPARSE);
    force = conf.getBoolean(ParserJob.FORCE_KEY, false);
    skipTruncated = conf.getBoolean(ParserJob.SKIP_TRUNCATED, true);

    LOG.info(StringUtil.formatParams(
        "batchId", batchId,
        "resume", resume,
        "reparse", reparse,
        "force", force,
        "skipTruncated", skipTruncated
    ));
  }

  @Override
  public void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
    String url = TableUtil.unreverseUrl(key);

    if (!shouldProcess(key, url, page)) {
      return;
    }

    Parse parse = parseUtil.process(key, page);

    ParseStatus pstatus = page.getParseStatus();

    // LOG.info("page status : " + ParseStatusCodes.majorCodes[pstatus.getMajorCode()]);

    // LOG.info("outlinks : " + parse.getOutlinks().length);

    countParseStatus(pstatus);

    getCounter().updateAffectedRows(url);

    context.write(key, page);
  }

  // 0 : "notparsed", 1 : "success", 2 : "failed"
  private void countParseStatus(ParseStatus pstatus) {
    if (pstatus == null) {
      return;
    }

    Counter counter = Counter.parseSuccess;

    switch (pstatus.getMajorCode()) {
      case 0 : counter = Counter.notParsed;
      case 1 : counter = Counter.parseSuccess;
      default : counter = Counter.parseFailed;
    }

    getCounter().increase(counter);
  }
  
  private boolean shouldProcess(String key, String url, WebPage page) {
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
  public static boolean isTruncated(String url, WebPage page) {
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
      LOG.warn(url + " skipped. Content of size " + inHeaderSize + " was truncated to " + actualSize);
      return true;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(url + " actualSize=" + actualSize + " inHeaderSize=" + inHeaderSize);
    }

    return false;
  }
} // ParserMapper
