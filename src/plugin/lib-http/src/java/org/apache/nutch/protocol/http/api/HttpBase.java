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
package org.apache.nutch.protocol.http.api;

// JDK imports
import crawlercommons.robots.BaseRobotRules;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.fetcher.FetchMode;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.net.proxy.NoProxyException;
import org.apache.nutch.net.proxy.ProxyPool;
import org.apache.nutch.net.proxy.ProxyPoolFactory;
import org.apache.nutch.protocol.*;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.DeflateUtils;
import org.apache.nutch.util.GZIPUtils;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

// crawler-commons imports

public abstract class HttpBase implements Protocol {

  private final static Utf8 RESPONSE_TIME = new Utf8("_rs_");

  public static final int BUFFER_SIZE = 8 * 1024;

  private static final byte[] EMPTY_CONTENT = new byte[0];

  private HttpRobotRulesParser robots = null;

  /** The proxy hostname. */
  protected String proxyHost = null;

  /** The proxy port. */
  protected int proxyPort = 8080;

  /** Indicates if a proxy is used */
  protected boolean useProxy = false;

  /** Indicates if a proxy pool is used */
  protected boolean useProxyPool = false;

  /** The proxy pool */
  protected ProxyPool proxyPool = null;

  /** The network timeout in millisecond */
  protected int timeout = 10000;

  /**
   * The max retry time if there is a network problem especially when uses a
   * proxy pool
   */
  protected int fetchMaxRetry = 5;

  /** The length limit for downloaded content, in bytes. */
  protected int maxContent = 64 * 1024;

  /** The Nutch 'User-Agent' request header */
  protected String userAgent = getAgentString("NutchCVS", null, "Nutch",
      "http://nutch.apache.org/bot.html", "agent@nutch.apache.org");

  /** The "Accept-Language" request header value. */
  protected String acceptLanguage = "en-us,en-gb,en;q=0.7,*;q=0.3";

  /** The "Accept" request header value. */
  protected String accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

  /** The default logger */
  private final static Logger LOGGER = LoggerFactory.getLogger(HttpBase.class);

  /** The specified logger */
  private Logger logger = LOGGER;

  /** The nutch configuration */
  private Configuration conf = null;

  /** The fetch mode */
  private FetchMode fetchMode = null;

  private MimeUtil mimeTypes;

  /** Do we use HTTP/1.1? */
  protected boolean useHttp11 = false;

  /** Response Time */
  protected boolean responseTime = true;

  /** Which TLS/SSL protocols to support */
  protected Set<String> tlsPreferredProtocols;

  /** Which TLS/SSL cipher suites to support */
  protected Set<String> tlsPreferredCipherSuites;

  /** Creates a new instance of HttpBase */
  public HttpBase() {
    this(null);
  }

  /** Creates a new instance of HttpBase */
  public HttpBase(Logger logger) {
    if (logger != null) {
      this.logger = logger;
    }
    robots = new HttpRobotRulesParser();
  }

  // Inherited Javadoc
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.fetchMode = FetchMode.fromString(conf.get("fetcher.fetch.mode", "native"));
    this.proxyHost = conf.get("http.proxy.host");
    this.proxyPort = conf.getInt("http.proxy.port", 8080);
    this.useProxyPool = FetchMode.PROXY.equals(conf.get("fetcher.fetch.mode"));
    if (this.useProxyPool) {
      this.proxyPool = new ProxyPoolFactory(conf).getProxyPool();
    }
    this.useProxy = (proxyHost != null && proxyHost.length() > 0) || this.useProxyPool;
    this.timeout = conf.getInt("http.timeout", 10000);
    this.fetchMaxRetry = conf.getInt("http.fetch.max.retry", 5);
    this.maxContent = conf.getInt("http.content.limit", 64 * 1024);
//    this.userAgent = getAgentString(conf.get("http.agent.name"),
//        conf.get("http.agent.version"), conf.get("http.agent.description"),
//        conf.get("http.agent.url"), conf.get("http.agent.email"));
    this.userAgent = getAgentString(conf.get("http.agent.name"));

    this.acceptLanguage = conf.get("http.accept.language", acceptLanguage);
    this.accept = conf.get("http.accept", accept);
    this.mimeTypes = new MimeUtil(conf);
    this.useHttp11 = conf.getBoolean("http.useHttp11", false);
    this.responseTime = conf.getBoolean("http.store.responsetime", true);
    this.robots.setConf(conf);

    String[] protocols = conf.getStrings("http.tls.supported.protocols",
        "TLSv1.2", "TLSv1.1", "TLSv1", "SSLv3");
    String[] ciphers = conf.getStrings("http.tls.supported.cipher.suites",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA", "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA", "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
        "SSL_RSA_WITH_RC4_128_SHA", "TLS_ECDH_ECDSA_WITH_RC4_128_SHA",
        "TLS_ECDH_RSA_WITH_RC4_128_SHA",
        "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA",
        "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
        "SSL_DHE_DSS_WITH_3DES_EDE_CBC_SHA", "SSL_RSA_WITH_RC4_128_MD5",
        "TLS_EMPTY_RENEGOTIATION_INFO_SCSV", "TLS_RSA_WITH_NULL_SHA256",
        "TLS_ECDHE_ECDSA_WITH_NULL_SHA", "TLS_ECDHE_RSA_WITH_NULL_SHA",
        "SSL_RSA_WITH_NULL_SHA", "TLS_ECDH_ECDSA_WITH_NULL_SHA",
        "TLS_ECDH_RSA_WITH_NULL_SHA", "SSL_RSA_WITH_NULL_MD5",
        "SSL_RSA_WITH_DES_CBC_SHA", "SSL_DHE_RSA_WITH_DES_CBC_SHA",
        "SSL_DHE_DSS_WITH_DES_CBC_SHA", "TLS_KRB5_WITH_RC4_128_SHA",
        "TLS_KRB5_WITH_RC4_128_MD5", "TLS_KRB5_WITH_3DES_EDE_CBC_SHA",
        "TLS_KRB5_WITH_3DES_EDE_CBC_MD5", "TLS_KRB5_WITH_DES_CBC_SHA",
        "TLS_KRB5_WITH_DES_CBC_MD5");

    tlsPreferredProtocols = new HashSet<String>(Arrays.asList(protocols));
    tlsPreferredCipherSuites = new HashSet<String>(Arrays.asList(ciphers));

    logger.info(StringUtil.formatParams(
        "fetcher.fetch.mode", fetchMode,
        "http.proxy.host", proxyHost,
        "http.proxy.port", proxyPort,
        "http.timeout", timeout,
        "http.content.limit", maxContent,
        "http.agent", userAgent,
        "http.accept.language", acceptLanguage,
        "http.accept", accept
    ));
  }

  // Inherited Javadoc
  public Configuration getConf() {
    return this.conf;
  }

  public ProtocolOutput getProtocolOutput(String url, WebPage page) {
    try {
      URL u = new URL(url);
      Response response = null; // make a request

      long startTime = System.currentTimeMillis();

      int retry = 0;
      while (response == null && retry < this.fetchMaxRetry) {
        try {
          if (retry > 0) {
            logger.debug("fetch {}, retry : {}", url, retry);
          }

          response = getResponse(u, page, false);
        } catch (NoProxyException | SocketException | SocketTimeoutException | EOFException e) {
          ++retry;
          response = null;
          logger.warn("{}, retry : {}", e, retry);
        } finally {
        }
      }

      // TODO : wrong elapsedTime in crowdsourcing mode
      int elapsedTime = (int) (System.currentTimeMillis() - startTime);

      if (response == null) {
        String e = "failed to fetch : " + url;
        logger.error(e);
        return new ProtocolOutput(null, ProtocolStatusUtils.makeStatus(ProtocolStatusCodes.EXCEPTION, e));
      }

      if (this.responseTime) {
        if (fetchMode.equals(FetchMode.CROWDSOURCING)) {
          elapsedTime = NumberUtils.toInt(response.getHeader("Q-Response-Time"), -1);
        }

        page.getMetadata().put(RESPONSE_TIME, ByteBuffer.wrap(Bytes.toBytes(elapsedTime)));
      }

      int code = response.getCode();
      byte[] content = response.getContent();
      Content c = new Content(u.toString(), u.toString(),
          (content == null ? EMPTY_CONTENT : content),
          response.getHeader("Content-Type"), response.getHeaders(), mimeTypes);

      if (code == 200) { // got a good response
        return new ProtocolOutput(c); // return it
      } else if (code >= 300 && code < 400) { // handle redirect
        String location = response.getHeader("Location");
        // some broken servers, such as MS IIS, use lowercase header name...
        if (location == null)
          location = response.getHeader("location");
        if (location == null)
          location = "";
        u = new URL(u, location);
        int protocolStatusCode;
        switch (code) {
        case 300: // multiple choices, preferred value in Location
          protocolStatusCode = ProtocolStatusCodes.MOVED;
          break;
        case 301: // moved permanently
        case 305: // use proxy (Location is URL of proxy)
          protocolStatusCode = ProtocolStatusCodes.MOVED;
          break;
        case 302: // found (temporarily moved)
        case 303: // see other (redirect after POST)
        case 307: // temporary redirect
          protocolStatusCode = ProtocolStatusUtils.TEMP_MOVED;
          break;
        case 304: // not modified
          protocolStatusCode = ProtocolStatusUtils.NOTMODIFIED;
          break;
        default:
          protocolStatusCode = ProtocolStatusUtils.MOVED;
        }
        // handle this in the higher layer.
        return new ProtocolOutput(c, ProtocolStatusUtils.makeStatus(
            protocolStatusCode, u));
      } else if (code == 400) { // bad request, mark as GONE
        if (logger.isTraceEnabled()) {
          logger.trace("400 Bad request: " + u);
        }
        return new ProtocolOutput(c, ProtocolStatusUtils.makeStatus(
            ProtocolStatusCodes.GONE, u));
      } else if (code == 401) { // requires authorization, but no valid auth
                                // provided.
        if (logger.isTraceEnabled()) {
          logger.trace("401 Authentication Required");
        }
        return new ProtocolOutput(c, ProtocolStatusUtils.makeStatus(
            ProtocolStatusCodes.ACCESS_DENIED, "Authentication required: "
                + url));
      } else if (code == 404) {
        return new ProtocolOutput(c, ProtocolStatusUtils.makeStatus(
            ProtocolStatusCodes.NOTFOUND, u));
      } else if (code == 410) { // permanently GONE
        return new ProtocolOutput(c, ProtocolStatusUtils.makeStatus(
            ProtocolStatusCodes.GONE, "Http: " + code + " url=" + u));
      } else {
        return new ProtocolOutput(c, ProtocolStatusUtils.makeStatus(
            ProtocolStatusCodes.EXCEPTION, "Http code=" + code + ", url=" + u));
      }
    } catch (Throwable e) {
      logger.error("Failed with the following error: ", e);
      return new ProtocolOutput(null, ProtocolStatusUtils.makeStatus(
          ProtocolStatusCodes.EXCEPTION, e.toString()));
    }
  }

  /*
   * -------------------------- * </implementation:Protocol> *
   * --------------------------
   */
  public String getProxyHost() {
    return proxyHost;
  }

  public int getProxyPort() {
    return proxyPort;
  }

  public boolean useProxy() {
    return useProxy;
  }

  public boolean useProxyPool() {
    return useProxyPool;
  }

  public ProxyPool proxyPool() {
    return proxyPool;
  }

  public int getTimeout() {
    return timeout;
  }

  public int getMaxContent() {
    return maxContent;
  }

  public String getUserAgent() {
    return userAgent;
  }

  /**
   * Value of "Accept-Language" request header sent by Nutch.
   * 
   * @return The value of the header "Accept-Language" header.
   */
  public String getAcceptLanguage() {
    return acceptLanguage;
  }

  public String getAccept() {
    return accept;
  }

  public boolean getUseHttp11() {
    return useHttp11;
  }

  public Set<String> getTlsPreferredCipherSuites() {
    return tlsPreferredCipherSuites;
  }

  public Set<String> getTlsPreferredProtocols() {
    return tlsPreferredProtocols;
  }

  private static String getAgentString(String agentName) {
    return agentName;
  }

  private static String getAgentString(String agentName, String agentVersion,
      String agentDesc, String agentURL, String agentEmail) {

    if ((agentName == null) || (agentName.trim().length() == 0)) {
      // TODO : NUTCH-258
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("No User-Agent string set (http.agent.name)!");
      }
    }

    StringBuffer buf = new StringBuffer();

    buf.append(agentName);
    if (agentVersion != null) {
      buf.append("/");
      buf.append(agentVersion);
    }
    if (((agentDesc != null) && (agentDesc.length() != 0))
        || ((agentEmail != null) && (agentEmail.length() != 0))
        || ((agentURL != null) && (agentURL.length() != 0))) {
      buf.append(" (");

      if ((agentDesc != null) && (agentDesc.length() != 0)) {
        buf.append(agentDesc);
        if ((agentURL != null) || (agentEmail != null))
          buf.append("; ");
      }

      if ((agentURL != null) && (agentURL.length() != 0)) {
        buf.append(agentURL);
        if (agentEmail != null)
          buf.append("; ");
      }

      if ((agentEmail != null) && (agentEmail.length() != 0))
        buf.append(agentEmail);

      buf.append(")");
    }
    return buf.toString();
  }

  public byte[] processGzipEncoded(byte[] compressed, URL url)
      throws IOException {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("uncompressing....");
    }

    byte[] content;
    if (getMaxContent() >= 0) {
      content = GZIPUtils.unzipBestEffort(compressed, getMaxContent());
    } else {
      content = GZIPUtils.unzipBestEffort(compressed);
    }

    if (content == null)
      throw new IOException("unzipBestEffort returned null");

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("fetched " + compressed.length
          + " bytes of compressed content (expanded to " + content.length
          + " bytes) from " + url);
    }
    return content;
  }

  public byte[] processDeflateEncoded(byte[] compressed, URL url)
      throws IOException {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("inflating....");
    }

    byte[] content = DeflateUtils
        .inflateBestEffort(compressed, getMaxContent());

    if (content == null)
      throw new IOException("inflateBestEffort returned null");

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("fetched " + compressed.length
          + " bytes of compressed content (expanded to " + content.length
          + " bytes) from " + url);
    }
    return content;
  }

  protected static void main(HttpBase http, String[] args) throws Exception {
    @SuppressWarnings("unused")
    boolean verbose = false;
    String url = null;

    String usage = "Usage: Http [-verbose] [-timeout N] url";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-timeout")) { // found -timeout option
        http.timeout = Integer.parseInt(args[++i]) * 1000;
      } else if (args[i].equals("-verbose")) { // found -verbose option
        verbose = true;
      } else if (i != args.length - 1) {
        System.err.println(usage);
        System.exit(-1);
      } else
        // root is required parameter
        url = args[i];
    }

    ProtocolOutput out = http.getProtocolOutput(url, WebPage.newBuilder().build());
    Content content = out.getContent();

    System.out.println("Status: " + out.getStatus());
    if (content != null) {
      System.out.println("Content Type: " + content.getContentType());
      System.out.println("Content Length: "
          + content.getMetadata().get(Response.CONTENT_LENGTH));
      System.out.println("Content:");
      String text = new String(content.getContent());
      System.out.println(text);
    }
  }

  // TODO : refactory this method
  @Override
  public void setResult(int status, Metadata headers, byte[] content) {
    
  }

  protected abstract Response getResponse(URL url, WebPage page,
      boolean followRedirects) throws ProtocolException, IOException, NoProxyException;

  @Override
  public BaseRobotRules getRobotRules(String url, WebPage page) {
    return robots.getRobotRulesSet(this, url);
  }
}
