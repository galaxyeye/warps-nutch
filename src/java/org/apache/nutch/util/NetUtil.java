package org.apache.nutch.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.*;

public class NetUtil {

  protected static final Logger logger = LoggerFactory.getLogger(NetUtil.class);

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
      return FileUtils.readFileToString(new File("/etc/hostname")).trim();
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
    String host = conf.get("nutch.master.domain", "localhost");
    int port = conf.getInt("nutch.server.port", 8182);

    return new URL("http", host, port, path);
  }

  public static String getBaseUrl(Configuration conf) {
    String host = conf.get("nutch.master.domain");
    int port = conf.getInt("nutch.server.port", 8081);

    return "http://" + host + ":" + port;
  }

  public static void main(String[] args) throws InterruptedException {
//    NetUtil.testNetwork("101.45.122.133", 19180);
//    NetUtil.testNetwork("192.168.1.101", 19180);
    NetUtil.testNetwork("127.0.0.1", 19080);
    // logger.info(proxyManager.toString());
  }
}
