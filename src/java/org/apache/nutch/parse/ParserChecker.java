/**
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

package org.apache.nutch.parse;

import com.google.common.collect.Sets;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.protocol.*;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Parser checker, useful for testing parser. It also accurately reports
 * possible fetching and parsing failures and presents protocol status signals
 * to aid debugging. The tool enables us to retrieve the following data from any
 * url:
 * <ol>
 * <li><tt>contentType</tt>: The URL {@link org.apache.nutch.protocol.Content}
 * type.</li>
 * <li><tt>signature</tt>: Digest is used to identify pages (like unique ID) and
 * is used to remove duplicates during the dedup procedure. It is calculated
 * using {@link org.apache.nutch.crawl.MD5Signature} or
 * {@link org.apache.nutch.crawl.TextProfileSignature}.</li>
 * <li><tt>Version</tt>: From {@link org.apache.nutch.parse}.</li>
 * <li><tt>Status</tt>: From {@link org.apache.nutch.parse}.</li>
 * <li><tt>Title</tt>: of the URL</li>
 * <li><tt>Outlinks</tt>: associated with the URL</li>
 * <li><tt>Content Metadata</tt>: such as <i>X-AspNet-Version</i>, <i>Date</i>,
 * <i>Content-length</i>, <i>servedBy</i>, <i>Content-Type</i>,
 * <i>Cache-Control</>, etc.</li>
 * <li><tt>Parse Metadata</tt>: such as <i>CharEncodingForConversion</i>,
 * <i>OriginalCharEncoding</i>, <i>language</i>, etc.</li>
 * <li><tt>ParseText</tt>: The page parse text which varies in length depdnecing
 * on <code>content.length</code> configuration.</li>
 * </ol>
 * 
 * @author John Xing
 */
public class ParserChecker extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(ParserChecker.class);

  private CrawlFilters crawlFilters;
  private URLFilters filters;
  private URLNormalizers normalizers;
  private Boolean dumpText;
  private Boolean force;
  private String contentType;
  private String url;

  public ParserChecker() {
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    dumpText = NutchUtil.getBoolean(args, "dumpText", false);
    force = NutchUtil.getBoolean(args, "force", false);
    url = NutchUtil.get(args, "url");

    if (url == null) {
      results.put("error", "Url must be specified");
    }

    crawlFilters = CrawlFilters.create(getConf());
    filters = new URLFilters(getConf());
    normalizers = new URLNormalizers(getConf(), URLNormalizers.SCOPE_OUTLINK);

    LOG.info(StringUtil.formatParams(
        "url", url,
        "dumpText", dumpText,
        "force", force
    ));
  }

  @Override
  public void doRun(Map<String, Object> args) throws Exception {
    WebPage page = getWebPage();

    // Parse parse = new ParseUtil(conf).parse(url, page);
    ParseUtil parseUtil = new ParseUtil(getConf());
    String key = TableUtil.reverseUrl(url);
    Parse parse = parseUtil.process(key, page);

    if (parse == null) {
      LOG.error("Problem with parse - check log");
      results.put("parseStatus", "error");
      return;
    }

    // Calculate the signature
    byte[] signature = SignatureFactory.getSignature(getConf()).calculate(page);

    if (LOG.isInfoEnabled()) {
      LOG.info("parsing: " + url);
      LOG.info("contentType: " + contentType);
      LOG.info("signature: " + StringUtil.toHexString(signature));
    }

    // LOG.info("---------\nMetadata\n---------\n");
    Map<CharSequence, ByteBuffer> metadata = page.getMetadata();
    StringBuffer sb = new StringBuffer();
    if (metadata != null) {
      Iterator<Entry<CharSequence, ByteBuffer>> iterator = metadata.entrySet().iterator();

      while (iterator.hasNext()) {
        Entry<CharSequence, ByteBuffer> entry = iterator.next();
        sb.append(entry.getKey().toString()).append(" : \t")
            .append(Bytes.toString(entry.getValue().array())).append("\n");
      }

      results.put("Metadata", sb);
    }

    Set<String> filteredOutlinks = Sets.newTreeSet();
    for (CharSequence link : page.getOutlinks().keySet()) {
      filteredOutlinks.add(link.toString());
    }
    results.put("Outlinks", StringUtils.join(filteredOutlinks, '\n'));

    // DiscardedOutlinks
    Set<String> discardedOutlinks = Sets.newTreeSet();
    for (Outlink outlink : parse.getOutlinks()) {
      String toUrl = outlink.getToUrl();
      toUrl = normalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
      toUrl = filters.filter(toUrl);
      if (toUrl == null) {
        discardedOutlinks.add(outlink.getToUrl());
      }
    }
    results.put("DiscardOutlinks", StringUtils.join(discardedOutlinks, '\n'));

    if (page.getHeaders() != null) {
      Map<CharSequence, CharSequence> headers = page.getHeaders();
      StringBuffer headersb = new StringBuffer();
      if (metadata != null) {
        Iterator<Entry<CharSequence, CharSequence>> iterator = headers.entrySet().iterator();

        while (iterator.hasNext()) {
          Entry<CharSequence, CharSequence> entry = iterator.next();
          headersb
            .append(entry.getKey().toString())
            .append(" : \t")
            .append(entry.getValue())
            .append("\n");
        }

        results.put("Headers", headersb);
      }
    }

    if (dumpText) {
      results.put("ParseText", parse.getText());
    }
  }

  private WebPage getWebPage() throws ProtocolNotFound {
    LOG.info("fetching: " + url);

    results.put("url", url);

    ProtocolFactory factory = new ProtocolFactory(getConf());
    Protocol protocol = factory.getProtocol(url);
    WebPage page = WebPage.newBuilder().build();

    ProtocolOutput protocolOutput = protocol.getProtocolOutput(url, page);

    if (!protocolOutput.getStatus().isSuccess()) {
      LOG.error("Fetch failed with protocol status: "
          + ProtocolStatusUtils.getName(protocolOutput.getStatus().getCode())
          + ": " + ProtocolStatusUtils.getMessage(protocolOutput.getStatus()));
      return null;
    }
    page.setStatus((int)CrawlStatus.STATUS_FETCHED);

    Content content = protocolOutput.getContent();

    if (content == null) {
      LOG.error("No content for " + url);
      results.put("content", content.toString());
      return null;
    }
    page.setBaseUrl(new org.apache.avro.util.Utf8(url));
    page.setContent(ByteBuffer.wrap(content.getContent()));

    if (force) {
      content.setContentType(contentType);
    } else {
      contentType = content.getContentType();
    }

    if (contentType == null) {
      LOG.error("Failed to determine content type!");
      return null;
    }

    page.setContentType(new Utf8(contentType));

    if (ParserMapper.isTruncated(url, page)) {
      LOG.warn("Content is truncated, parse may fail!");
      results.put("contentStatus", "truncated");
    }

    return page;
  }

  public int run(String[] args) throws Exception {
    String usage = "Usage: ParserChecker [-dumpText] [-forceAs mimeType] [-filter filterFile] url";

    if (args.length == 0) {
      LOG.error(usage);
      return (-1);
    }

    boolean dumpText = false;
    boolean force = false;
    String contentType = null;
    String filter = null;
    String url = null;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-forceAs")) {
        force = true;
        contentType = args[++i];
      } else if (args[i].equals("-dumpText")) {
        dumpText = true;
      } else if (args[i].equals("-filter")) {
        filter = FileUtils.readFileToString(new File(args[++i]));
        getConf().set("crawl.outlink.filter.rules", filter);
        LOG.info("filter: " + filter);
      } else {
        url = URLUtil.toASCII(args[i]);
      }
    }

    run(StringUtil.toArgMap("dumpText", dumpText, "force", force, "contentType", contentType, "url", url));

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ParserChecker(), args);
    System.exit(res);
  }

}
