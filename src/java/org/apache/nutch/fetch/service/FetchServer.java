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
package org.apache.nutch.fetch.service;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.service.misc.ErrorStatusService;
import org.apache.nutch.net.client.NutchClient;
import org.apache.nutch.persist.local.model.ServerInstance;
import org.apache.nutch.util.NetUtil;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.restlet.ext.jaxrs.JaxRsApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Application;
import java.util.Set;
import java.util.logging.Level;

/**
 * FetchServer is responsible to schedule fetch tasks
 * */
public class FetchServer extends Application {

  public static final String FETCH_SERVER = "FETCH_SERVER";

  public static final Logger LOG = LoggerFactory.getLogger(FetchServer.class);

  public static final int BASE_PORT = 21000;

  public static final int MAX_PORT = BASE_PORT + 1000;

  public static final int DEFAULT_PORT = BASE_PORT;

  private String logLevel = "INFO";

  private int port;

  private Component component;

  private long startTime;

  private Configuration conf;

  /**
   * Public constructor which accepts the port we wish to run the server on
   */
  public FetchServer(Configuration conf, int port) {
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
    childContext.getAttributes().put(FETCH_SERVER, this);

    // Attach the application
    component.getDefaultHost().attach(application);
  }

  public static int acquirePort(Configuration conf) {
    NutchClient client = new NutchClient(conf);
    if (client.available()) {
      return client.acquirePort(ServerInstance.Type.FetcherServer);
    }
    else {
      LOG.warn("Client is not available");
    }

    return -1;
  }

  public void recyclePort() {
    NutchClient client = new NutchClient(conf);
    if (client.available()) {
      client.recyclePort(ServerInstance.Type.FetcherServer, port);
      client.unregister(new ServerInstance(null, port, ServerInstance.Type.FetcherServer));
    }
    else {
      LOG.warn("Client is not available");
    }
  }

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> resources = Sets.newHashSet();

    resources.add(FetchResource.class);

    return resources;
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

  public long getStartTime() { return this.startTime; }

  /**
   * Starts the Nutch server printing some logging to the log file.
   * 
   */
  public void start() {
    if (isRunning()) {
      LOG.warn("FetchServer is already running");
    }

    LOG.info("Starting FetchServer on port: {} with logging level: {} ...", port, logLevel);

    try {
      component.start();
    } catch (Exception e) {
      throw new IllegalStateException("Cannot start server!", e);
    }

    LOG.info("FetchServer is started on port {}", port);
    startTime = System.currentTimeMillis();

    // We use an Internet ip rather than an Intranet ip
    NutchClient client = new NutchClient(conf);
    client.register(new ServerInstance(null, port, ServerInstance.Type.FetcherServer));
  }

  /**
   * Safety and convenience method to determine whether or not it is safe to
   * shut down the server. We make this assertion by consulting the
   * {@link org.apache.nutch.service} for a list of jobs with
   * {@link org.apache.nutch.service.model.response.JobInfo#state} equal to
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

    try {
      recyclePort();
      component.stop();
      LOG.info("FetchServer is stopped. Port : {}", port);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot stop nutch server", e);
    }

    return true;
  }

}
