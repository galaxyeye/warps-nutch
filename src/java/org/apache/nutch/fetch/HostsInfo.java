package org.apache.nutch.fetch;

import com.google.common.collect.Multiset;

/**
 * Created by vincent on 16-10-14.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class HostsInfo {
  private Multiset<String> resourceCategories;
  private Multiset<String> unreachableHosts;
  private Multiset<String> availableHosts;
}
