package org.apache.nutch.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.fetch.data.FetchEntry;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Nutch.*;


/**
 * <p>
 * Mapper class for SimpleFetcher.
 * </p>
 * <p>
 * This class reads the random integer written by {@link GenerateJob} as its
 * key while outputting the actual key and value arguments through a
 * {@link FetchEntry} instance.
 * </p>
 * <p>
 * This approach (combined with the use of PartitionUrlByHost makes
 * sure that SimpleFetcher is still polite while also randomizing the key order. If
 * one host has a huge number of URLs in your table while other hosts have
 * not, {@link FetchReducer} will not be stuck on one host but process URLs
 * from other hosts as well.
 * </p>
 */
public class FetchMapper extends NutchMapper<String, WebPage, IntWritable, FetchEntry> {

  public enum Counter { notGenerated, alreadyFetched, hostsUnreachable };

  private boolean resume;
  private int limit = -1;
  private int count = 0;

  private Random random = new Random();
  private Set<String> unreachableHosts = new HashSet<>();
  private NutchMetrics nutchMetrics;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    Configuration conf = context.getConfiguration();

    String crawlId = conf.get(PARAM_CRAWL_ID);
    String fetchMode = conf.get(PARAM_FETCH_MODE);
    int numTasks = conf.getInt(PARAM_MAPREDUCE_JOB_REDUCES, 2);
    limit = conf.getInt(PARAM_MAPPER_LIMIT, -1);
    limit = limit < 2 * numTasks ? limit : limit/numTasks;

    resume = conf.getBoolean(PARAM_RESUME, false);
    this.nutchMetrics = new NutchMetrics(conf);

    boolean ignoreUnreachableHosts = conf.getBoolean("fetcher.fetch.mapper.ignore.unreachable.hosts", true);
    if (ignoreUnreachableHosts) {
      nutchMetrics.loadUnreachableHosts(unreachableHosts);
    }

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "fetchMode", fetchMode,
        "resume", resume,
        "numTasks", numTasks,
        "limit", limit,
        "ignoreUnreachableHosts", ignoreUnreachableHosts,
        "unreachableHostsPath", nutchMetrics.getUnreachableHostsPath(),
        "unreachableHosts", unreachableHosts.size()
    ));
  }

  /**
   * Rows are filtered by batchId first in FetchJob setup, which can be a range search, the time complex is O(ln(N))
   * and then filtered by mapper, which is a scan, the time complex is O(N)
   * */
  @Override
  protected void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
    getCounter().increase(rows);

    String url = TableUtil.unreverseUrl(key);

    if (unreachableHosts.contains(URLUtil.getDomainName(url))) {
      getCounter().increase(Counter.hostsUnreachable);
      // LOG.debug("Skipping " + url + ", host is in unreachable list");
      return;
    }

    if (!Mark.GENERATE_MARK.hasMark(page)) {
      getCounter().increase(Counter.notGenerated);
      // LOG.debug("Skipping " + url + "; not generated yet");
      return;
    }

    /**
     * Resume the batch, but ignore rows that are already fetched.
     * If FetchJob runs again but no resume flag set, the pages already fetched should be fetched again.
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

    context.write(new IntWritable(random.nextInt(65536)), new FetchEntry(conf, key, page));

    getCounter().updateAffectedRows(url);

    // LOG.debug("SimpleFetcher mapper : " + key);

    if (limit > 0 && ++count > limit) {
      stop("Hit limit " + limit + ", finish the mapper.");
    }
  }
}
