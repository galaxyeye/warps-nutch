package org.apache.nutch.filter;

import com.google.gson.annotations.Expose;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.nutch.util.DomUtil;
import org.w3c.dom.Node;

import java.util.Set;

public class BlockFilter {
  @Expose
  private Set<String> allow;

  @Expose
  private Set<String> disallow;

  public BlockFilter() {
    
  }

  public boolean isDisallowed(Node node) {
    // TODO : use real css selector
    Set<String> simpleSelectors = DomUtil.getSimpleSelectors(node);

    // System.out.println("simpleSelectors : " + simpleSelectors);

    if (!CollectionUtils.isEmpty(disallow) 
        && CollectionUtils.containsAny(disallow, simpleSelectors)) {
      return true;
    }

    return false;
  }

  public boolean isAllowed(Node node) {
    Set<String> simpleSelectors = DomUtil.getSimpleSelectors(node);

    if (CollectionUtils.isEmpty(allow) 
        || CollectionUtils.containsAny(allow, simpleSelectors)) {
      return true;
    }

    return false;
  }

  public Set<String> getAllow() {
    return allow;
  }

  public void setAllow(Set<String> allow) {
    this.allow = allow;
  }

  public Set<String> getDisallow() {
    return disallow;
  }

  public void setDisallow(Set<String> disallow) {
    this.disallow = disallow;
  }

  public String toString() {
    return "\n\tallow" + allow + "\n\tdisallow" + disallow;
  }
}
