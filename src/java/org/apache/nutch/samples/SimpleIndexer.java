package org.apache.nutch.samples;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.common.Params;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexWriters;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilters;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

/**
 * Created by vincent on 16-9-8.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class SimpleIndexer {

  public static final Logger LOG = LoggerFactory.getLogger(SimpleIndexer.class);

  private Configuration conf;
  private String solrUrl;
  private SimpleFetcher fetcher;
  private SimpleParser parser;
  private IndexingFilters indexingFilters;
  private IndexWriters indexWriters;

  public SimpleIndexer(Configuration conf) throws IOException {
    this.conf = conf;
    this.fetcher = new SimpleFetcher(conf);
    this.parser = new SimpleParser(conf);
    this.indexingFilters = new IndexingFilters(conf);
    this.solrUrl = conf.get(Nutch.PARAM_SOLR_SERVER_URL);

    if (solrUrl != null) {
      indexWriters = new IndexWriters(conf);
      indexWriters.open(conf);
    }

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "solrUrl", solrUrl,
        "plugin.includes", conf.get("plugin.includes"),
        "indexingFilters", indexingFilters
    ));
  }

  public IndexDocument index(String url) {
    return index(url, null);
  }

  public IndexDocument index(String url, String contentType) {
    return index(url, contentType, 0);
  }

  public IndexDocument index(String url, String contentType, int depth) {
    IndexDocument doc = null;

    try {
      WebPage page = fetcher.fetch(url, contentType);
      if (page == null) {
        return null;
      }

      String reversedUrl = TableUtil.reverseUrl(url);
      parser.parse(page);
      doc = indexingFilters.filter(new IndexDocument(reversedUrl), url, page);
      if (indexWriters != null) {
        indexWriters.write(doc);
        page.putIndexTimeHistory(Instant.now());
      }

      System.out.println(doc);

      if (depth > 0) {
        int d = --depth;
        page.getOutlinks().keySet().stream().map(CharSequence::toString)
            .filter(link -> !link.matches("(.+)(jpg|png|gif|js|css|json)"))
            .filter(link -> link.length() > Nutch.SHORTEST_VALID_URL_LENGTH)
            .forEach(link -> index(link, contentType, d));
      }
    } catch (ProtocolNotFound|IndexingException |IOException e) {
      LOG.error(e.getMessage());
    }

    return doc;
  }

  public void commit() throws IOException {
    if (indexWriters != null) {
      indexWriters.commit();
    }
  }

  public void close() throws IOException {
    if (indexWriters != null) {
      indexWriters.commit();
      indexWriters.close();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage : SimpleIndexer [-forceAs contentType] [-solrUrl solrUrl] [-depth depth] url");
      System.exit(0);
    }

    Configuration conf = ConfigUtils.create();
    conf.set("plugin.includes", "protocol-(http)|urlfilter-regex|parse-(html|tika)|index-(metadata|basic|anchor|more)|indexer-solr|urlnormalizer-(pass|regex|basic)|scoring-opic");

    String contentType = null;
    String url = null;
    String solrUrl;
    int depth = 0;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-forceAs")) {
        contentType = args[++i];
      }
      else if (args[i].equals("-depth")) {
        depth = StringUtil.tryParseInt(args[++i]);
      }
      else if (args[i].equals("-solrUrl")) {
        solrUrl = args[++i];
        conf.set(Nutch.PARAM_SOLR_SERVER_URL, solrUrl);
      }
      else {
        url = URLUtil.toASCII(args[i]);
      }
    }

    SimpleIndexer indexer = new SimpleIndexer(conf);
    indexer.index(url, contentType, depth);
    indexer.close();
  }
}
