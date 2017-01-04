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

package org.apache.nutch.jobs.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.jobs.NutchJob;
import org.apache.nutch.samples.SimpleParser;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

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
public class ParserCheckJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(ParserCheckJob.class);

  private Boolean dumpText;
  private Boolean force;
  private String url;

  public ParserCheckJob() {
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Params params = new Params(args);
    Configuration conf = getConf();

    dumpText = params.getBoolean("dumpText", false);
    force = params.getBoolean("force", false);
    url = params.get("url");

    if (url == null) {
      results.put("error", "Url must be specified");
    }

    LOG.info(Params.format(
        "url", url,
        "dumpText", dumpText,
        "force", force
    ));
  }

  @Override
  public void doRun(Map<String, Object> args) throws Exception {
    SimpleParser parser = new SimpleParser(getConf());
    parser.parse(url);
    results.putAll(parser.getResult());
  }

  public int run(String[] args) throws Exception {
    String usage = "Usage: ParserCheckJob [-dumpText] [-forceAs mimeType] [-filter filterFile] url";

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
        filter = new String(Files.readAllBytes(Paths.get(args[++i])));
        getConf().set("crawl.outlink.filter.rules", filter);
        LOG.info("filter: " + filter);
      } else {
        url = URLUtil.toASCII(args[i]);
      }
    }

    run(Params.toArgMap("dumpText", dumpText, "force", force, "contentType", contentType, "url", url));

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(ConfigUtils.create(), new ParserCheckJob(), args);
    System.exit(res);
  }

}
