package org.apache.nutch.util;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.mapreduce.ParserMapper;
import org.apache.nutch.protocol.*;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

public class NetUtil {

  protected static final Logger LOG = LoggerFactory.getLogger(NetUtil.class);

  public static int ProxyConnectionTimeout = 5 * 1000;

  public static boolean testNetwork(String ip, int port) {
    return testTcpNetwork(ip, port);
  }

  public static boolean testHttpNetwork(String ip, int port) {
    boolean reachable = false;

    try {
      URL url = new URL("http", ip, port, "/");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setConnectTimeout(ProxyConnectionTimeout);
      con.connect();
      // logger.info("available proxy server {} : {}", ip, port);
      reachable = true;
      con.disconnect();
    } catch (Exception x) {
      // logger.warn("can not connect to " + ip + ":" + port);
    }

    return reachable;
  }

  public static boolean testTcpNetwork(String ip, int port) {
    boolean reachable = false;
    Socket con = new Socket();

    try {
      con.connect(new InetSocketAddress(ip, port), ProxyConnectionTimeout);
      // logger.info("available proxy server : " + ip + ":" + port);
      reachable = true;
      con.close();
    } catch (Exception e) {
      // logger.warn("can not connect to " + ip + ":" + port);
    }

    return reachable;
  }

  /**
   * TODO : use package org.apache.nutch.util.domain
   * */
  public static String getTopLevelDomain(String baseUri) {
    if (baseUri == null || baseUri.startsWith("/") || baseUri.startsWith("file://")) {
      return null;
    }

    int pos = -1;
    String domain = null;

    if (StringUtils.startsWith(baseUri, "http")) {
      final int fromIndex = "https://".length();
      pos = baseUri.indexOf('/', fromIndex);

      if (pos != -1)
        baseUri = baseUri.substring(0, pos);
    }

    pos = baseUri.indexOf(".") + 1;

    if (pos != 0) {
      domain = baseUri.substring(pos);
    }

    // for example : http://dangdang.com
    if (domain != null && !domain.contains(".")) {
      domain = baseUri.replaceAll("(http://|https://)", "");
    }

    return domain;
  }

  public static String getHostname() {
    try {
      return Files.readAllLines(Paths.get("/etc/hostname")).get(0);
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * TODO : We may need a better solution to indicate whether it's a master
   * */
  public static boolean isMaster(Configuration conf) {
    String masterHostname = conf.get("nutch.master.hostname", "localhost");
    if (masterHostname.equals("localhost") || masterHostname.equals(NetUtil.getHostname())) {
      return true;
    }

    return false;
  }

  public static URL getUrl(Configuration conf, String path) throws MalformedURLException {
    String host = conf.get("nutch.master.host", "localhost");
    int port = conf.getInt("nutch.server.port", 8182);

    return new URL("http", host, port, path);
  }

  public static String getBaseUrl(Configuration conf) {
    String host = conf.get("nutch.master.host");
    int port = conf.getInt("nutch.server.port", 8081);

    return "http://" + host + ":" + port;
  }

  public static WebPage getWebPage(String url, String contentType, Configuration conf) throws ProtocolNotFound {
    LOG.info("fetching: " + url);

    ProtocolFactory factory = new ProtocolFactory(conf);
    Protocol protocol = factory.getProtocol(url);
    WebPage page = WebPage.newWebPage();

    ProtocolOutput protocolOutput = protocol.getProtocolOutput(url, page);

    if (!protocolOutput.getStatus().isSuccess()) {
      LOG.warn("Fetch failed with protocol status: "
          + ProtocolStatusUtils.getName(protocolOutput.getStatus().getCode())
          + ": " + ProtocolStatusUtils.getMessage(protocolOutput.getStatus()));
      return null;
    }
    page.setStatus((int)CrawlStatus.STATUS_FETCHED);

    Content content = protocolOutput.getContent();

    if (content == null) {
      LOG.warn("No content for " + url);
      return null;
    }
    page.setBaseUrl(url);
    page.setContent(content.getContent());

    if (contentType != null) {
      content.setContentType(contentType);      
    }
    else {
      contentType = content.getContentType();
    }

    if (contentType == null) {
      LOG.warn("Failed to determine content type!");
      return null;
    }

    page.setContentType(contentType);

    if (ParserMapper.isTruncated(url, page)) {
      LOG.warn("Content is truncated, parse may fail!");
    }

    return page;
  }
}
