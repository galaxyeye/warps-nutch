package org.apache.nutch.net.proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NetUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProxyUpdateThread extends Thread {

  protected static final Logger logger = LoggerFactory.getLogger(ProxyUpdateThread.class);
  protected long updatePeriod = 10 * 1000;

  /** The nutch configuration */
  private final Configuration conf;

  private ProxyPool proxyPool;

  public ProxyUpdateThread(Configuration conf) {
    this.conf = conf;
    this.proxyPool = new ProxyPoolFactory(this.conf).getProxyPool();
    this.updatePeriod = conf.getInt("http.proxy.pool.update.period", 10 * 1000);
    this.setDaemon(true);

    proxyPool.tryUpdateFromFile();
  }

  @Override
  public void run() {
    if (proxyPool == null) {
      logger.error("proxy manager must not be null");
      return;
    }

    try {
      int tick = 0;
      while (true) {
        if (tick % 20 == 0) {
          logger.debug("updating proxy pool...");
        }

        if (tick % 20 == 0) {
          updateProxyConfigFileFromMaster();
        }

        long start = System.currentTimeMillis();
        proxyPool.reviewRetired();
        long elapsed = System.currentTimeMillis() - start;

        // too often, enlarge review period
        if (elapsed > updatePeriod) {
          logger.info("it costs {} millis to check all retired proxy servers, enlarge the check interval", elapsed);
          updatePeriod = elapsed * 10;
        }

        proxyPool.tryUpdateFromFile();
        Thread.sleep(updatePeriod);

        ++tick;
      }
    } catch (InterruptedException e) {
      logger.error(e.toString());
    }
  }

  private void updateProxyConfigFileFromMaster() {
    String host = conf.get("nutch.master");
    int port = conf.getInt("nutch.server.port", 8081);
    String url = "http://" + host + ":" + port + "/proxy/download";

    try {
      String hostname = NetUtil.getHostname();

      // update only if this is not the master
      if (!hostname.equals(host)) {
        // TODO : avoid using system command
        String cmd = "wget " + url + " -O " + ProxyPool.ProxyListFile + " > /dev/null 2>&1";
        Process p = Runtime.getRuntime().exec(cmd);
      }
    } catch (IOException e) {
      logger.error(e.toString());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = NutchConfiguration.create();

    ProxyUpdateThread updateThread = new ProxyUpdateThread(conf);
    updateThread.start();

    ProxyPool proxyPool = new ProxyPoolFactory(conf).getProxyPool();

    while (true) {
      ProxyEntry proxy = proxyPool.poll();

      if (ProxyPool.testNetwork(proxy)) {
        logger.debug("proxy : {} is available", proxy);
      }
      else {
        logger.debug("proxy : {} is not available", proxy);
      }

      Thread.sleep(3000);
    }
  }
}
