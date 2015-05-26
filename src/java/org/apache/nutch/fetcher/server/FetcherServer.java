/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.fetcher.server;

import java.util.Set;
import java.util.logging.Level;

import javax.ws.rs.core.Application;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.misc.ErrorStatusService;
import org.apache.nutch.client.NutchClient;
import org.apache.nutch.storage.local.model.ServerInstance;
import org.apache.nutch.util.NetUtil;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.restlet.ext.jaxrs.JaxRsApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class FetcherServer extends Application {

  public static final String FETCHER_SERVER = "FETCHER_SERVER";

  public static final Logger LOG = LoggerFactory.getLogger(FetcherServer.class);

  public static final int BASE_PORT = 21000;

  public static final int MAX_PORT = BASE_PORT + 1000;

  public static final int DEFAULT_PORT = BASE_PORT;

  private String logLevel = "INFO";

  private Integer port;

  private Component component;

  private long startTime;

  private Configuration conf;

  /**
   * Public constructor which accepts the port we wish to run the server on as
   * well as the logging granularity. If the latter option is not provided via
   * {@link org.apache.nutch.api.ServerInstance#main(String[])} then it defaults to
   * 'INFO' however best attempts should always be made to specify a logging
   * level.
   */
  public FetcherServer(Configuration conf, int port) {
    this.conf = conf;
    this.port = port;

    // Create a new Component.
    component = new Component();
    component.getLogger().setLevel(Level.parse(logLevel));

    // Add a new HTTP server listening on defined port.
    component.getServers().add(Protocol.HTTP, this.port);

    Context childContext = component.getContext().createChildContext();
    JaxRsApplication application = new JaxRsApplication(childContext);
    application.add(this);
    application.setStatusService(new ErrorStatusService());
    childContext.getAttributes().put(FETCHER_SERVER, this);

    // Attach the application
    component.getDefaultHost().attach(application);
  }

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> resources = Sets.newHashSet();

    resources.add(FetcherResource.class);

    return resources;
  }

  public static FetcherServer startServer(Configuration conf, int port) {
    if (!isRunning(port)) {
      FetcherServer server = new FetcherServer(conf, port);
      server.start();

      return server;
    }

    return null;
  }

  public static FetcherServer startInDaemonThread(final Configuration conf, final int port) {
    if (isRunning(port)) {
      return null;
    }

    FetcherServerThread thread = new FetcherServerThread(conf, port);
    thread.setDaemon(true);
    thread.start();

    return thread.getServer();
  }

  private static class FetcherServerThread extends Thread {
    private FetcherServer server;
    private final Configuration conf;
    private final int port;

    public FetcherServerThread(final Configuration conf, final int port) {
      this.conf = conf;
      this.port = port;
    }

    @Override
    public void run() {
      server = startServer(conf, port);
    }

    public FetcherServer getServer() {
      return server;
    }
  }

  /**
   * Convenience method to determine whether a Nutch server is running.
   * 
   * @return true if a server instance is running.
   */
  public static boolean isRunning(int port) {
    return NetUtil.testNetwork("127.0.0.1", port);
  }

  public boolean isRunning() {
    if (component != null && component.isStarted()) {
      return true;
    }

    return isRunning(this.port);
  }

  /**
   * Starts the Nutch server printing some logging to the log file.
   * 
   */
  public void start() {
    if (isRunning()) {
      LOG.info("FetcherServer is already running");
    }

    LOG.info("Starting FetcherServer on port: {} with logging level: {} ...", port, logLevel);

    try {
      component.start();
    } catch (Exception e) {
      throw new IllegalStateException("Cannot start server!", e);
    }

    LOG.info("Started FetcherServer on port {}", port);
    startTime = System.currentTimeMillis();

    // We need an Internet ip rather than an Intranet ip
    NutchClient client = new NutchClient(conf,
        conf.get("nutch.master.domain", "localhost"),
        conf.getInt("nutch.server.port", 8182));
    client.register(new ServerInstance(null, port, ServerInstance.Type.FetcherServer));
  }

  /**
   * Safety and convenience method to determine whether or not it is safe to
   * shut down the server. We make this assertion by consulting the
   * {@link org.apache.nutch.api.NutchApp#jobManager} for a list of jobs with
   * {@link org.apache.nutch.api.model.response.JobInfo#state} equal to
   * 'RUNNING'.
   * 
   * @param force
   *          ignore running tasks
   * 
   * @return true if there are no jobs running or false if there are jobs with
   *         running state.
   */
  public boolean canStop(boolean force) {
    if (force) {
      return true;
    }

    return true;
  }

  /**
   * Stop the Nutch server.
   * 
   * @param force
   *          boolean method to effectively kill jobs regardless of state.
   * @return true if no server is running or if the shutdown was successful.
   *         Return false if there are running jobs and the force switch has not
   *         been activated.
   */
  public boolean stop(boolean force) {
    if (!canStop(force)) {
      LOG.warn("Running jobs - can't stop now.");
      return false;
    }

    LOG.info("Stopping FetcherServer on port {}...", port);
    try {
      component.stop();

      LOG.info("Stopped FetcherServer on port {}", port);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot stop nutch server", e);
    }

    NutchClient client = new NutchClient(conf,
        conf.get("nutch.master.domain", "localhost"),
        conf.getInt("nutch.server.port", 8182));
    client.unregister(new ServerInstance(null, port, ServerInstance.Type.FetcherServer));
    client.recyclePort(ServerInstance.Type.FetcherServer, port);

    return true;
  }

  public static Integer acquirePort(Configuration conf) {
    NutchClient client = new NutchClient(conf);
    if (client.available()) {
      return client.acquirePort(ServerInstance.Type.FetcherServer);
    }

    return DEFAULT_PORT;
  }
}
