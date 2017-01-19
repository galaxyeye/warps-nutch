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

package org.apache.nutch.samples;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.fetch.FetchUtil;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.ProtocolStatus;
import org.apache.nutch.persist.gora.WebPageConverter;
import org.apache.nutch.protocol.*;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Parser checker, useful for testing parser. It also accurately reports
 * possible fetching and parsing failures and presents protocol status signals
 * to aid debugging. The tool enables us to retrieve the following data from any
 */
public class SimpleFetcher extends Configured {

  public static final Logger LOG = LoggerFactory.getLogger(SimpleFetcher.class);

  public SimpleFetcher(Configuration conf) {
    setConf(conf);
  }

  public WebPage fetch(String url) throws ProtocolNotFound {
    return fetch(url, null);
  }

  public WebPage fetch(String url, String contentType) throws ProtocolNotFound {
    LOG.info("Fetching: " + url);

    ProtocolFactory factory = new ProtocolFactory(getConf());
    Protocol protocol = factory.getProtocol(url);
    WebPage page = WebPage.newWebPage();

    ProtocolOutput protocolOutput = protocol.getProtocolOutput(url, page);
    ProtocolStatus pstatus = protocolOutput.getStatus();

    if (!pstatus.isSuccess()) {
      LOG.error("Fetch failed with protocol status, "
          + ProtocolStatusUtils.getName(pstatus.getCode())
          + " : " + ProtocolStatusUtils.getMessage(pstatus));
      return null;
    }

    Content content = protocolOutput.getContent();

    FetchUtil.updateStatus(page, CrawlStatus.STATUS_FETCHED, pstatus);
    FetchUtil.updateContent(page, content);
    FetchUtil.updateFetchTime(page);
    FetchUtil.updateMarks(page);

    if (content == null) {
      LOG.error("No content for " + url);
      return null;
    }

    saveWebPage(page);

    return page;
  }

  private void saveWebPage(WebPage page) {
    try {
      Path path = Paths.get("/tmp/nutch/web/" + DigestUtils.md5Hex(page.getBaseUrl()));

      if (!Files.exists(path)) {
        FileUtils.forceMkdir(path.getParent().toFile());
        Files.createFile(path);
      }
      Files.write(path, page.getContent().array());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage : SimpleParser [-forceAs contentType] url");
      System.exit(0);
    }

    String contentType = null;
    String url = null;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-forceAs")) {
        contentType = args[++i];
      } else {
        url = URLUtil.toASCII(args[i]);
      }
    }

    SimpleFetcher fetcher = new SimpleFetcher(ConfigUtils.create());
    WebPage page = fetcher.fetch(url, contentType);
    System.out.println(page.getText());

    System.out.println(WebPageConverter.convert(page));
  }
}
