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
package org.apache.nutch.service;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.client.NutchClient;
import org.apache.nutch.service.impl.ConfManagerImpl;
import org.apache.nutch.service.impl.JobFactory;
import org.apache.nutch.service.impl.JobManagerImpl;
import org.apache.nutch.service.impl.JobWorkerPoolExecutor;
import org.apache.nutch.service.misc.ErrorStatusService;
import org.apache.nutch.service.model.response.JobInfo;
import org.apache.nutch.service.model.response.JobInfo.State;
import org.apache.nutch.service.resources.*;
import org.apache.nutch.persist.local.SpringConfiguration;
import org.apache.nutch.persist.local.model.ServerInstance;
import org.apache.nutch.persist.local.service.BrowserInstanceService;
import org.apache.nutch.persist.local.service.ServerInstanceService;
import org.apache.nutch.util.NetUtil;
import org.apache.nutch.util.ConfigUtils;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.ext.jaxrs.JaxRsApplication;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.ws.rs.core.Application;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.apache.nutch.metadata.Nutch.DEFAULT_MASTER_HOSTNAME;
import static org.apache.nutch.metadata.Nutch.DEFAULT_MASTER_PORT;
import static org.apache.nutch.metadata.Nutch.PARAM_NUTCH_MASTER_HOST;

public class NutchMaster extends Application {
  public static final String NUTCH_SERVER = "NUTCH_SERVER";

  public static final Logger LOG = LoggerFactory.getLogger(NutchMaster.class);

  private static final String LOCALHOST = "localhost";
  private static final String DEFAULT_LOG_LEVEL = "INFO";
  private static final Integer DEFAULT_PORT = 8182;
  private static final int JOB_CAPACITY = 100;

  private static String logLevel = DEFAULT_LOG_LEVEL;
  private static Integer port = DEFAULT_PORT;

  private static final String CMD_HELP = "help";
  private static final String CMD_STOP = "stop";
  private static final String CMD_PORT = "port";
  private static final String CMD_LOG_LEVEL = "log";

  private ApplicationContext springContext;
  private Component component;
  private Configuration conf;
  private ConfManager configManager;
  private JobManager jobManager;
  private long startTime;

  private AtomicBoolean running = new AtomicBoolean(false);
  private ServerInstance serverInstance;

  /**
   * Public constructor which accepts the port we wish to run the server on as
   * well as the logging granularity. If the latter option is not provided via
   * {@link NutchMaster#main(String[])} then it defaults to
   * 'INFO' however best attempts should always be made to specify a logging
   * level.
   */
  public NutchMaster() {
    // Hadoop configuration
    configManager = new ConfManagerImpl();
    conf = configManager.getDefault();

    // Spring configuration
    springContext = new AnnotationConfigApplicationContext(SpringConfiguration.class);
    ServerInstanceService serverInstanceService = springContext.getBean(ServerInstanceService.class);
    serverInstanceService.truncate();
    BrowserInstanceService browserInstanceService = springContext.getBean(BrowserInstanceService.class);
    browserInstanceService.truncate();

    // Nutch job container
    BlockingQueue<Runnable> runnables = Queues.newArrayBlockingQueue(JOB_CAPACITY);
    JobWorkerPoolExecutor executor = new JobWorkerPoolExecutor(10, JOB_CAPACITY, 1, TimeUnit.HOURS, runnables);
    jobManager = new JobManagerImpl(new JobFactory(), executor, configManager);

    // Nutch server
    // Create a new Component.
    component = new Component();
    component.getLogger().setLevel(Level.parse(logLevel));

    // Add a new HTTP server listening on defined port.
    port = conf.getInt("nutch.server.port", DEFAULT_PORT);
    component.getServers().add(Protocol.HTTP, port);

    Context childContext = component.getContext().createChildContext();
    JaxRsApplication application = new JaxRsApplication(childContext);
    application.add(this);
    application.setStatusService(new ErrorStatusService());
    childContext.getAttributes().put(NUTCH_SERVER, this);

    // Attach the application.
    component.getDefaultHost().attach(application);
  }

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> resources = Sets.newHashSet();

    resources.add(JobResource.class);
    resources.add(AdminResource.class);
    resources.add(ConfigResource.class);
    resources.add(DbResource.class);
    resources.add(SeedResource.class);
    resources.add(ProxyResource.class);
    resources.add(PortResource.class);
    resources.add(ServiceResource.class);

    return resources;
  }

  public ApplicationContext getSpringContext() {
    return springContext;
  }

  public ConfManager getConfManager() {
    return configManager;
  }

  public JobManager getJobManager() {
    return jobManager;
  }

  public long startTime() {
    return startTime;
  }

  public static void startServer(Configuration conf) {
    if (!isRunning(conf)) {
      NutchMaster server = new NutchMaster();
      server.start();
    }
    else {
      LOG.warn("Nutch master is already running.");
    }
  }

  public static Thread startAsDaemon(Configuration conf) {
    if (!NetUtil.isMaster(conf) || NutchMaster.isRunning(conf)) {
      return null;
    }

    Thread thread = new Thread() {
      @Override
      public void run() {
        startServer(conf);
      }
    };

    thread.setDaemon(true);
    thread.start();

    return thread;
  }

  /**
   * Convenience method to determine whether a Nutch server is running.
   * 
   * @return true if a server instance is running.
   */
  public static boolean isRunning(Configuration conf) {
    String master = conf.get(PARAM_NUTCH_MASTER_HOST, DEFAULT_MASTER_HOSTNAME);

    return NetUtil.testNetwork(master, port);
  }

  /**
   * Starts the Nutch server printing some logging to the log file.
   */
  public void start() {
    if (isRunning(conf)) {
      LOG.info("NutchMaster is already running");
    }

    LOG.info("Starting NutchMaster on port: {} with logging level: {} ...", port, logLevel);

    try {
      component.start();
    } catch (Exception e) {
      throw new IllegalStateException("Cannot start server!", e);
    }

    LOG.info("Started NutchMaster on port {}", port);
    running.set(true);
    startTime = System.currentTimeMillis();

    // use a intenet hostname/domain/ip to enable internet access by a satellite
    String master = conf.get("nutch.master.host", DEFAULT_MASTER_HOSTNAME);
    int port = conf.getInt("nutch.server.port", DEFAULT_MASTER_PORT);
    registerFectchServerInstance(master, port, conf);
  }

  /**
   * @param host
   *          if host is a internet ip or a internet hostname, the fetch server can be accessed 
   *          from internet, or if host is a interact ip or a internet hostname
   * @param port
   *          nutch master port   
   */
  public void registerFectchServerInstance(String host, int port, Configuration conf) {
    NutchClient client = new NutchClient(conf, host, DEFAULT_MASTER_PORT);
    serverInstance = client.register(new ServerInstance(null, port, ServerInstance.Type.NutchServer));
  }

  /**
   * Safety and convenience method to determine whether or not it is safe to
   * shut down the server. We make this assertion by consulting the
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

    Collection<JobInfo> jobs = getJobManager().list(null, State.RUNNING);
    return jobs.isEmpty();
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
    if (!running.get()) {
      return true;
    }

    if (!canStop(force)) {
      LOG.warn("Running jobs - can't stop now.");
      return false;
    }

    LOG.info("Stopping NutchMaster on port {}...", port);
    try {
      new NutchClient(conf).unregister(serverInstance);

      component.stop();

      running.set(false);

      LOG.info("Stopped NutchMaster on port {}", port);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot stop nutch server", e);
    }

    return true;
  }

  private static void stopRemoteServer(boolean force) {
    Reference reference = new Reference(Protocol.HTTP, LOCALHOST, port);
    reference.setPath("/admin/stop");

    if (force) {
      reference.addQueryParameter("force", "true");
    }

    ClientResource clientResource = new ClientResource(reference);
    clientResource.get();
  }

  private static Options createOptions() {
    Options options = new Options();
    OptionBuilder.hasArg();
    OptionBuilder.withArgName("logging level");
    OptionBuilder.withDescription(
        "Select a logging level for the NutchMaster: \n" + "ALL|CONFIG|FINER|FINEST|INFO|OFF|SEVERE|WARNING");
    options.addOption(OptionBuilder.create(CMD_LOG_LEVEL));

    OptionBuilder.withDescription("Stop running NutchMaster. "
        + "true value forces the Server to stop despite running jobs e.g. kills the tasks ");
    OptionBuilder.hasOptionalArg();
    OptionBuilder.withArgName("force");
    options.addOption(OptionBuilder.create(CMD_STOP));

    OptionBuilder.withDescription("Show this help");
    options.addOption(OptionBuilder.create(CMD_HELP));

    OptionBuilder.withDescription("Port to use for restful API.");
    OptionBuilder.hasOptionalArg();
    OptionBuilder.withArgName("port number");
    options.addOption(OptionBuilder.create(CMD_PORT));
    return options;
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new PosixParser();
    Options options = createOptions();
    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption(CMD_HELP)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("NutchMaster", options, true);
      return;
    }

    if (commandLine.hasOption(CMD_LOG_LEVEL)) {
      logLevel = commandLine.getOptionValue(CMD_LOG_LEVEL);
    }

    if (commandLine.hasOption(CMD_PORT)) {
      port = Integer.parseInt(commandLine.getOptionValue(CMD_PORT));
    }

    if (commandLine.hasOption(CMD_STOP)) {
      String stopParameter = commandLine.getOptionValue(CMD_STOP);
      boolean force = StringUtils.equals("force", stopParameter);
      stopRemoteServer(force);
      return;
    }

    startServer(ConfigUtils.create());
  }
}
