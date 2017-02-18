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
package org.apache.nutch.graph;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.persist.WebPage;
import org.jgrapht.graph.DefaultWeightedEdge;

public class WebEdge extends DefaultWeightedEdge {

  private String anchor = "";
  private Metadata metadata = new Metadata();

  public WebEdge() {}

  public String getAnchor() { return anchor; }

  public void setAnchor(CharSequence anchor) {
    if (anchor == null) {
      anchor = "";
    }
    this.anchor = anchor.toString();
  }

  public void addMetadata(String k, String v) { metadata.add(k, v); }

  public String getMetadata(String k) { return metadata.get(k); }

  public boolean hasMetadata(String k) { return getMetadata(k) != null; }

  public Metadata getMetadata() { return metadata; }

  public void setMetadata(Metadata metadata) { this.metadata = metadata; }

  public boolean isLoop() { return getSource().equals(getTarget()); }

  /**
   * Retrieves the source of this edge.
   *
   * @return source of this edge
   */
  public WebVertex getSource()
  {
    return (WebVertex)super.getSource();
  }

  public String getSourceUrl() { return getSource().getUrl(); }

  public WebPage getSourceWebPage() { return getSource().getWebPage(); }

  public boolean hasSourceWebPage() { return getSource().hasWebPage(); }

  /**
   * Retrieves the target of this edge.
   *
   * @return target of this edge
   */
  public WebVertex getTarget() { return (WebVertex)super.getTarget(); }

  public String getTargetUrl() { return getTarget().getUrl(); }

  public WebPage getTargetWebPage() { return getTarget().getWebPage(); }

  public boolean hasTargetWebPage() { return getTarget().hasWebPage(); }

  @Override
  public String toString() { return getSourceUrl() + " -> " + getTargetUrl(); }
}
