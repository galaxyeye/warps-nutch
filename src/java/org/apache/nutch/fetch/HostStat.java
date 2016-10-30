package org.apache.nutch.fetch;

import org.apache.nutch.util.TableUtil;

/**
 * Created by vincent on 16-10-15.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 * */
public class HostStat implements Comparable<HostStat> {

  public HostStat(String hostName) {
    this.hostName = hostName;
  }

  public String hostName;
  public int urls = 0;
  public int indexUrls = 0;
  public int detailUrls = 0;
  public int searchUrls = 0;
  public int mediaUrls = 0;
  public int bbsUrls = 0;
  public int blogUrls = 0;
  public int tiebaUrls = 0;
  public int urlsTooLong = 0;
  public int urlsFromSeed = 0;

  @Override
  public int compareTo(HostStat hostStat) {
    if (hostName == null || hostStat == null || hostStat.hostName == null) {
      return -1;
    }

    String reverseHost = TableUtil.reverseHost(hostName);
    String reverseHost2 = TableUtil.reverseHost(hostStat.hostName);

    return reverseHost.compareTo(reverseHost2);
  }
}
