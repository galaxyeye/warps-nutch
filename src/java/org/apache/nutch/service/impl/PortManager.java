package org.apache.nutch.service.impl;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nutch.persist.local.model.ServerInstance;
import org.apache.nutch.util.NetUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class PortManager {

  private ServerInstance.Type type;
  private int basePort;
  private int maxPort;

  private AtomicInteger nextPort = new AtomicInteger(0);
  private Set<Integer> activePorts = Sets.newHashSet();
  private Set<Integer> freePorts = Sets.newHashSet();

  public PortManager(ServerInstance.Type type, int basePort, int maxPort) {
    this.type = type;
    this.basePort = basePort;
    this.maxPort = maxPort;
    this.nextPort.set(basePort);
  }

  public ServerInstance.Type type() {
    return type;
  }

  public synchronized List<Integer> activePorts() {
    List<Integer> ports = Lists.newArrayList();

    ports.addAll(activePorts);

    return ports;
  }

  public synchronized List<Integer> freePorts() {
    List<Integer> ports = Lists.newArrayList();

    ports.addAll(activePorts);

    return ports;
  }

  public synchronized Integer acquire() {
    Integer port = null;

    if (!freePorts.isEmpty()) {
      port = freePorts.iterator().next();
      freePorts.remove(port);
    }
    else {
      port = nextPort.incrementAndGet();
    }

    if (port >= basePort && port <= maxPort) {
      activePorts.add(port);
    }
    else {
      port = -1;
    }

    if (NetUtil.testNetwork("127.0.0.1", port)) {
      port = -1;
    }

    return port;
  }

  public synchronized void recycle(int port) {
    activePorts.remove(port);
    freePorts.add(port);
  }
}
