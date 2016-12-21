package org.apache.nutch.persist.graph;

/**
 * Created by vincent on 16-12-21.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public enum GraphMode {
  IN_LINK_GRAPH, OUT_LINK_GRAPH;
  public boolean isInLinkGraph() { return this == IN_LINK_GRAPH; }
  public boolean isOutLinkGraph() { return this == OUT_LINK_GRAPH; }
}
