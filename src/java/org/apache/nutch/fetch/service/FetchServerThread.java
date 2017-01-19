package org.apache.nutch.fetch.service;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by vincent on 17-1-20.
 */
public class FetchServerThread extends Thread {
  private FetchServer server;
  private final Configuration conf;
  private final int port;

  public FetchServerThread(Configuration conf, int port) {
    this.conf = conf;
    this.port = port;
    setDaemon(true);
  }

  @Override
  public void run() {
    if (!FetchServer.isRunning(port)) {
      server = new FetchServer(conf, port);
      server.start();
    }
  }

  public FetchServer getServer() {
    return server;
  }
}
