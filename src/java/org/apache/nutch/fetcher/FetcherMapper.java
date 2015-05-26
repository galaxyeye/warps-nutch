package org.apache.nutch.fetcher;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.mapreduce.NutchMapper;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;


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
 * This approach (combined with the use of {@link PartitionUrlByHost}) makes
 * sure that Fetcher is still polite while also randomizing the key order. If
 * one host has a huge number of URLs in your table while other hosts have
 * not, {@link FetcherReducer} will not be stuck on one host but process URLs
 * from other hosts as well.
 * </p>
 */
public class FetcherMapper extends NutchMapper<String, WebPage, IntWritable, FetchEntry> {

  public static enum Counter { rows, notGenerated, alreadyFetched };

  private boolean resume;

  private Random random = new Random();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    resume = conf.getBoolean(FetcherJob.RESUME_KEY, false);

    getCounter().register(Counter.class);
  }

  @Override
  protected void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
    if (!Mark.GENERATE_MARK.hasMark(page)) {
      getCounter().increase(Counter.notGenerated);

      if (LOG.isDebugEnabled()) {
        // LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; not generated yet");
      }

      return;
    }

    if (resume && Mark.FETCH_MARK.hasMark(page)) {
      getCounter().increase(Counter.alreadyFetched);

      if (LOG.isDebugEnabled()) {
        // LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; already fetched");
      }

      return;
    }

    getCounter().increase(Counter.rows);

    context.write(new IntWritable(random.nextInt(65536)), new FetchEntry(conf, key, page));
  }
}
