package org.apache.nutch.net.proxy;

import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProxyEntry implements Comparable<ProxyEntry> {

  public static final String IpPortRegex = "^((25[0-5]|2[0-4]\\d|1?\\d?\\d)\\.){3}(25[0-5]|2[0-4]\\d|1?\\d?\\d)(:[0-9]{2,5})";
  public static final Pattern IpPortPattern = Pattern.compile(IpPortRegex);

  protected static int DefaultProxyServerPort = 19080;

  // check if the proxy server is still available if it's not used for 10 seconds
  protected int proxyExpired = 10 * 1000; // a heart beating mechanism

  // if a proxy server can not be connected in a hour, we announce it's dead and remove it from the file
  private long missingProxyDeadTime = 60 * 60 * 1000;

  private String host;
  private int port;
  private long lastAvailableTime = 0;
  private boolean available = true;

  public ProxyEntry(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public ProxyEntry(String host, int port, long lastAvailableTime) {
    this.host = host;
    this.port = port;
    this.lastAvailableTime = lastAvailableTime;
  }

  public String host() { return host; }
  public int port() { return port; }
  // public long lastAvailableTime() { return lastAvailableTime; }
  public void refresh() { refresh(true); }
  public void refresh(boolean available) {
    this.available = available;
    if (available) {
      this.lastAvailableTime= System.currentTimeMillis();
    }
  }
  public boolean available() {
    return available;
  }
  public long lastAvailableTime() {
    return lastAvailableTime;
  }
  public boolean expired() {
    return System.currentTimeMillis() - lastAvailableTime > proxyExpired;
  }

  public boolean dead() {
    return lastAvailableTime > 0 &&
        System.currentTimeMillis() - lastAvailableTime > missingProxyDeadTime;
  }

  public static ProxyEntry parse(String ipPort) {
    String host = null;
    int port = DefaultProxyServerPort;
    long lastAvailableTime = 0;

    Matcher matcher = IpPortPattern.matcher(ipPort);
    if (!matcher.find()) {
      return null;
    }

    String timeString = ipPort.substring(matcher.end()).trim();
    if (timeString.matches("\\d+")) {
      lastAvailableTime = Long.parseLong(timeString);
    }

    ipPort = matcher.group();
    int pos = ipPort.lastIndexOf(':');
    if (pos != -1) {
      host = ipPort.substring(0, pos);
      port = Integer.parseInt(ipPort.substring(pos + 1));
    }

    return new ProxyEntry(host, port, lastAvailableTime);
  }

  public static boolean validateIpPort(String ipPort) {
    return ipPort.matches(IpPortRegex);
  }

  public String ipPort() {
    return host + ":" + port;
  }

  @Override
  public String toString() {
    return ipPort() + " " + lastAvailableTime;
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof ProxyEntry) && ipPort().equals(((ProxyEntry) o).ipPort());
  }

  @Override
  public int compareTo(ProxyEntry proxyEntry) {
    return ipPort().compareTo(proxyEntry.ipPort());
  }

  public static void main(String[] args) {
    String lines[] = {
        "192.144.22.123:89081",
        "192.144.22.123:89082 182342323",
        "192.144.22.123:89082 182341234",
        "192.144.22.123:89082 190341226",
        "192.144.22.123:89083 adasdfadf",
    };

    Set<ProxyEntry> mergedProxyEntries = new TreeSet<ProxyEntry>();
    for (String line : lines) {
      mergedProxyEntries.add(ProxyEntry.parse(line));
    }

    System.out.println(mergedProxyEntries);
  }
}
