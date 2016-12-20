package org.apache.nutch.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.fetch.data.FetchEntry;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
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
public class FetchMapper extends NutchMapper<String, GoraWebPage, IntWritable, FetchEntry> {

  public enum Counter { 
    notGenerated, alreadyFetched, hostsUnreachable,
    rowsIsSeed, rowsInjected,
    rowsDepth0, rowsDepth1, rowsDepth2, rowsDepth3, rowsDepthN
  };

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
    this.nutchMetrics = NutchMetrics.getInstance(conf);

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
  protected void map(String key, GoraWebPage row, Context context) throws IOException, InterruptedException {
    getCounter().increase(rows);

    String url = TableUtil.unreverseUrl(key);
    WebPage page = WebPage.wrap(row);

    if (unreachableHosts.contains(URLUtil.getDomainName(url))) {
      getCounter().increase(Counter.hostsUnreachable);
      return;
    }

    if (!page.hasMark(Mark.GENERATE)) {
      getCounter().increase(Counter.notGenerated);
      return;
    }

    /*
     * Resume the batch, but ignore rows that are already fetched.
     * If FetchJob runs again but no resume flag set, the pages already fetched should be fetched again.
     *
     * NOTE : Nutch removes marks only in DbUpdatejob, include INJECT, GENERATE, FETCH, PARSE,
     * so a page row can have multiple marks.
     * */
    if (resume && page.hasMark(Mark.FETCH)) {
      getCounter().increase(Counter.alreadyFetched);
      return;
    }

    int priority = page.getFetchPriority(FETCH_PRIORITY_DEFAULT);
    // Higher priority, comes first
    int shuffleOrder = random.nextInt(65536) - 65536 * priority;
    context.write(new IntWritable(shuffleOrder), new FetchEntry(conf, key, page));
    updateStatus(url, page);

    if (limit > 0 && ++count > limit) {
      stop("Hit limit " + limit + ", finish the mapper.");
    }
  }

  private void updateStatus(String url, WebPage page) throws IOException, InterruptedException {
    Counter counter;
    int depth = page.getDepth();
    if (depth == 0) {
      counter = Counter.rowsDepth0;
    }
    else if (depth == 1) {
      counter = Counter.rowsDepth1;
    }
    else if (depth == 2) {
      counter = Counter.rowsDepth2;
    }
    else if (depth == 3) {
      counter = Counter.rowsDepth3;
    }
    else {
      counter = Counter.rowsDepthN;
    }

    getCounter().increase(counter);

    if (page.isSeed()) {
      getCounter().increase(Counter.rowsIsSeed);
    }

    if (page.hasMark(Mark.INJECT)) {
      getCounter().increase(Counter.rowsInjected);
    }

    getCounter().updateAffectedRows(url);
  }
}
