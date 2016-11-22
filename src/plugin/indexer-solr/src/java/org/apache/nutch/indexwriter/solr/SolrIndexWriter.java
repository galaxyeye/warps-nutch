/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexwriter.solr;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexField;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.mapreduce.IndexJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.metadata.SolrConstants;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TimingUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nutch.metadata.Nutch.PARAM_NUTCH_JOB_NAME;

public class SolrIndexWriter implements IndexWriter {

  public static final Logger LOG = LoggerFactory.getLogger(SolrIndexWriter.class);

  private Configuration conf;
  private String[] solrUrls = ArrayUtils.EMPTY_STRING_ARRAY;
  private String[] zkHosts = ArrayUtils.EMPTY_STRING_ARRAY;
  private String collection;
  private List<SolrClient> solrClients;
  private SolrMappingReader solrMapping;
  private ModifiableSolrParams params;

  private int batchSize;
  private int numDeletes = 0;
  private int totalAdds = 0;
  private int totalDeletes = 0;
  private int totalUpdates = 0;
  private boolean delete = false;
  private boolean writeFile = false;

  private String reportSuffix;
  private NutchMetrics nutchMetrics;

  private final List<SolrInputDocument> inputDocs = new ArrayList<>();
  private final List<SolrInputDocument> updateDocs = new ArrayList<>();
  private final List<String> deleteIds = new ArrayList<>();

  public SolrIndexWriter() {

  }

  public void open(JobConf jobConf, String name) {
    solrClients = SolrUtils.getSolrClients(solrUrls, zkHosts, collection);
  }

  public void open(Configuration jobConf) {
    solrClients = SolrUtils.getSolrClients(solrUrls, zkHosts, collection);
  }

  public void deleteByQuery(String query) throws IOException {
    try {
      LOG.info("SolrWriter: deleting " + query);
      for (SolrClient solrClient : solrClients) {
        solrClient.deleteByQuery(query);
      }
    } catch (final SolrServerException e) {
      LOG.error("Error deleting: " + deleteIds);
      throw makeIOException(e);
    }
  }

  @Override
  public void delete(String key) throws IOException {
    try {
      key = URLDecoder.decode(key, "UTF8");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error decoding: " + key);
      throw new IOException("UnsupportedEncodingException for " + key);
    } catch (IllegalArgumentException e) {
      LOG.warn("Could not decode: " + key + ", it probably wasn't encoded in the first place..");
    }

    // escape solr hash separator
    key = key.replaceAll("!", "\\!");

    if (delete) {
      deleteIds.add(key);
      totalDeletes++;
    }

    if (deleteIds.size() >= batchSize) {
      push();
    }
  }

  @Override
  public void update(IndexDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void write(IndexDocument doc) throws IOException {
    final SolrInputDocument inputDoc = new SolrInputDocument();

    for (final Entry<String, IndexField> e : doc) {
      for (final Object val : e.getValue().getValues()) {
        // normalise the string representation for a Date
        Object val2 = val;

        if (val instanceof Date) {
          val2 = TimingUtil.solrCompatibleFormat((Date)val2);
        }

        String key = solrMapping.mapKeyIfExists(e.getKey());

        if (key == null) {
          continue;
        }

        Boolean isMultiValued = solrMapping.isMultiValued(e.getKey());
        if (!isMultiValued) {
          if (inputDoc.getField(key) == null) {
            inputDoc.addField(key, val2, e.getValue().getWeight());
          }
        }
        else {
          inputDoc.addField(key, val2, e.getValue().getWeight());
        }
      } // for
    } // for

    inputDoc.setDocumentBoost(doc.getWeight());
    inputDocs.add(inputDoc);
    totalAdds++;

    if (inputDocs.size() + numDeletes >= batchSize) {
      push();
    }

    debugIndexDocTime(doc);
  }

  private void debugIndexDocTime(IndexDocument doc) {
    IndexDocument debugDoc = new IndexDocument();
    String[] debugFields = {"page_category", "last_crawl_time", "publish_time", "url"};
    Arrays.stream(debugFields).forEachOrdered(f -> debugDoc.addIfNotNull(f, doc.getFieldValue(f)));
    nutchMetrics.debugIndexDocTime(debugDoc.formatAsLine(), reportSuffix);
  }

  @Override
  public void close() throws IOException {
    commit();

    for (SolrClient solrClient : solrClients) {
      solrClient.close();
    }
  }

  @Override
  public void commit() throws IOException {
    if (inputDocs.isEmpty()) {
      return;
    }

    push();

    try {
      for (SolrClient solrClient : solrClients) {
        solrClient.commit();
      }
    } catch (Throwable e) {
      LOG.error("Failed to commit to solr : " + e.getMessage());
    }
  }

  public void push() throws IOException {
    if (inputDocs.size() > 0) {
      try {
        String message = "Indexing " + inputDocs.size() + "/" + totalAdds + " documents";
        if (numDeletes > 0) {
          message += ", deleting " + numDeletes + " ones";
        }
        LOG.info(message);

        numDeletes = 0;

        UpdateRequest req = new UpdateRequest();
        req.add(inputDocs);
        req.setAction(AbstractUpdateRequest.ACTION.OPTIMIZE, false, false);
        req.setParams(params);

        for (SolrClient solrClient : solrClients) {
          solrClient.request(req);
        }
      }
      catch (Throwable e) {
        LOG.error("Failed to write to solr " + e.toString());
      }

      inputDocs.clear();
    }

    if (deleteIds.size() > 0) {
      try {
        LOG.info("SolrIndexer: deleting "
            + String.valueOf(deleteIds.size()) + "/" + String.valueOf(totalDeletes) + " documents");

        for (SolrClient solrClient : solrClients) {
          solrClient.deleteById(deleteIds);
        }
      } catch (final SolrServerException e) {
        LOG.error("Error deleting: " + deleteIds);
        throw makeIOException(e);
      }

      deleteIds.clear();
    }
  }

  public static IOException makeIOException(SolrServerException e) {
    final IOException ioe = new IOException();
    ioe.initCause(e);
    return ioe;
  }

  private void writeToFile() {
    Path path = Paths.get("/tmp/nutch/index.txt");
    if (!path.toFile().exists()) {
      try {
        Files.createDirectories(path.getParent());
        Files.createFile(path);

        for (SolrInputDocument doc : inputDocs) {
          Files.write(path, "\n\n-----------------------------\n\n".getBytes(), StandardOpenOption.APPEND);
          Files.write(path, doc.toString().getBytes(), StandardOpenOption.APPEND);
        }
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }
  }

  /**
   * Debug solr request
   * */
  private void debug(UpdateRequest request) {
    String username = System.getenv("USER");
    Path path = Paths.get("/tmp", "scent-" + username, "indexer", "solr", "request." + TimingUtil.now("MdHms") + ".xml");
    try {
      FileUtils.forceMkdir(path.getParent().toFile());
      String xml = request.getXML();
      Files.write(path, xml.getBytes());
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    solrUrls = conf.getStrings(Nutch.PARAM_SOLR_SERVER_URL, ArrayUtils.EMPTY_STRING_ARRAY);
    zkHosts = conf.getStrings(Nutch.PARAM_SOLR_ZK, ArrayUtils.EMPTY_STRING_ARRAY);
    collection = conf.get(Nutch.PARAM_SOLR_COLLECTION);

    // dateFormat = DateFormat.getDateInstance(DateFormat.DEFAULT, Locale.ENGLISH);

    if (solrUrls == null && zkHosts == null) {
      String message = "Either SOLR URL or Zookeeper URL is required. " +
          "Use -D " + Nutch.PARAM_SOLR_SERVER_URL + " or -D " + Nutch.PARAM_SOLR_ZK;
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException("Failed to init SolrIndexWriter");
    }

    batchSize = conf.getInt(SolrConstants.COMMIT_SIZE, 250);
    delete = conf.getBoolean(IndexJob.INDEXER_DELETE, false);
    String paramString = conf.get(IndexJob.INDEXER_PARAMS);

    solrMapping = SolrMappingReader.getInstance(conf);
    // parse optional params
    params = new ModifiableSolrParams();
    if (paramString != null) {
      String[] values = paramString.split("&");
      for (String v : values) {
        String[] kv = v.split("=");
        if (kv.length < 2) {
          continue;
        }
        params.add(kv[0], kv[1]);
      }
    }

    this.reportSuffix = conf.get(PARAM_NUTCH_JOB_NAME, "job-unknown-" + TimingUtil.now("MMdd.HHmm"));
    this.nutchMetrics = NutchMetrics.getInstance(conf);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "batchSize", batchSize,
        "delete", delete,
        "params", params,
        "solrUrls", Stream.of(solrUrls).collect(Collectors.joining(",")),
        "zkHosts", Stream.of(zkHosts).collect(Collectors.joining(",")),
        "collection", collection
    ));
  }

  public String describe() {
    StringBuffer sb = new StringBuffer("SOLRIndexWriter\n");
    sb.append("\t").append(Nutch.PARAM_SOLR_SERVER_URL).append(" : URL of the SOLR instance\n");
    sb.append("\t").append(Nutch.PARAM_SOLR_ZK).append(" : URL of the Zookeeper quorum\n");
    sb.append("\t").append(Nutch.PARAM_SOLR_COLLECTION).append(" : SOLR collection\n");
    sb.append("\t").append(SolrConstants.COMMIT_SIZE).append(" : buffer size when sending to SOLR (default 1000)\n");
    sb.append("\t").append(SolrConstants.MAPPING_FILE)
        .append(" : name of the mapping file for fields (default solrindex-mapping.xml)\n");
    sb.append("\t").append(SolrConstants.USE_AUTH).append(" : use authentication (default false)\n");
    sb.append("\t").append(SolrConstants.USERNAME).append(" : username for authentication\n");
    sb.append("\t").append(SolrConstants.PASSWORD).append(" : password for authentication\n");
    return sb.toString();
  }
}
