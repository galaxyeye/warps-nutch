package org.apache.nutch.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.fetch.FetchUtil;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.ProtocolStatus;
import org.apache.nutch.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
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
   * TODO : use package org.apache.nutch.net.domain
   */
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
   */
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

  public static boolean isExternalLink(String sourceUrl, String destUrl) {
    try {
      String toHost = new URL(destUrl).getHost().toLowerCase();
      String fromHost = new URL(sourceUrl).getHost().toLowerCase();
      return !toHost.equals(fromHost);
    } catch (MalformedURLException ignored) {
    }

    return true;
  }

  public static WebPage fetch(String url, Configuration conf) {
    return fetch(url, null, conf);
  }

  public static WebPage fetch(String url, String contentType, Configuration conf) {
    ProtocolFactory factory = new ProtocolFactory(conf);
    Protocol protocol;
    try {
      protocol = factory.getProtocol(url);
    } catch (ProtocolNotFound protocolNotFound) {
      protocolNotFound.printStackTrace();
      return null;
    }

    WebPage page = WebPage.newWebPage(url);
    ProtocolOutput protocolOutput = protocol.getProtocolOutput(url, page);
    ProtocolStatus pstatus = protocolOutput.getStatus();

    if (!ProtocolStatusUtils.isSuccess(pstatus)) {
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

    return page;
  }
}
