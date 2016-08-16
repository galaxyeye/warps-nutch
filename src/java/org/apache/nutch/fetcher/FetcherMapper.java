package org.apache.nutch.fetcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.mapreduce.NutchMapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;

import java.io.IOException;
import java.util.Random;


/**
 * <p>
 * Mapper class for Fetcher.
 * </p>
 * <p>
 * This class reads the random integer written by {@link GeneratorJob} as its
 * key while outputting the actual key and value arguments through a
 * {@link FetchEntry} instance.
 * </p>
 * <p>
 * This approach (combined with the use of PartitionUrlByHost makes
 * sure that Fetcher is still polite while also randomizing the key order. If
 * one host has a huge number of URLs in your table while other hosts have
 * not, {@link FetcherReducer} will not be stuck on one host but process URLs
 * from other hosts as well.
 * </p>
 */
public class FetcherMapper extends NutchMapper<String, WebPage, IntWritable, FetchEntry> {

  public enum Counter { rows, notGenerated, alreadyFetched };

  private boolean resume;

  private Random random = new Random();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);

    resume = conf.getBoolean(FetcherJob.RESUME_KEY, false);

    getCounter().register(Counter.class);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "fetchMode", fetchMode,
        "resume", resume
    ));
  }

  /**
   * Rows are filtered by batchId first in FetchJob setup, which can be a range search, the time complex is O(ln(N))
   * and then filtered by mapper, which is a scan, the time complex is O(N)
   * */
  @Override
  protected void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
    if (!Mark.GENERATE_MARK.hasMark(page)) {
      getCounter().increase(Counter.notGenerated);

      if (LOG.isDebugEnabled()) {
        // LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; not generated yet");
      }

      return;
    }

    /**
     * Resume the batch, but ignore rows that are already fetched.
     * If FetcherJob runs again but no resume flag set, the pages already fetched should be fetched again.
     *
     * NOTE : Nutch removes marks only in DbUpdatejob, include INJECT_MARK, GENERATE_MARK, FETCH_MARK, PARSE_MARK,
     * so a page row can have multiple marks.
     * */
    if (resume && Mark.FETCH_MARK.hasMark(page)) {
      getCounter().increase(Counter.alreadyFetched);

      if (LOG.isDebugEnabled()) {
        // LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; already fetched");
      }

      return;
    }

    getCounter().increase(Counter.rows);

    context.write(new IntWritable(random.nextInt(65536)), new FetchEntry(conf, key, page));

    // LOG.debug("Fetcher mapper : " + key);

    /**
     * TODO : Consider also update the WebPage row, add a new mark FETCHING_MARK.
     * If we have FETCHING_MARK, we can fetch rows with "already generated" safely
     * (multiple.row)context.write(TableUtil.reverseUrl(key.url), page);
     * */
  }
}
