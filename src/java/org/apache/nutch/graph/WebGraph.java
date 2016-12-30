package org.apache.nutch.graph;

import org.apache.nutch.persist.WebPage;
import org.jgrapht.ext.*;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.DirectedPseudograph;

import java.util.*;

/**
 * Created by vincent on 16-12-21.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class WebGraph extends DefaultDirectedWeightedGraph<WebVertex, WebEdge> {

  public WebGraph() {
    super(WebEdge.class);
  }

  public WebEdge addVertexAndEdge(WebVertex source, WebVertex target) {
    addVertex(source);
    addVertex(target);
    return super.addEdge(source, target);
  }

  /**
   * Create exporter
   */
  public static GraphExporter<WebVertex, DefaultWeightedEdge> createExporter() {
    // create GraphML exporter
    GraphMLExporter<WebVertex, DefaultWeightedEdge> exporter = new GraphMLExporter<>(WebVertex::getUrl, null, DefaultEdge::toString, null);

    // set to export the internal edge weights
    exporter.setExportEdgeWeights(true);

    // register additional name attribute for vertices and edges
    exporter.registerAttribute("url", GraphMLExporter.AttributeCategory.ALL, GraphMLExporter.AttributeType.STRING);

    // register additional color attribute for vertices
    exporter.registerAttribute("depth", GraphMLExporter.AttributeCategory.NODE, GraphMLExporter.AttributeType.INT);

    // create provider of vertex attributes
    ComponentAttributeProvider<WebVertex> vertexAttributeProvider = v -> {
      Map<String, String> m = new HashMap<>();
      m.put("baseUrl", v.getWebPage().getBaseUrl());
      m.put("depth", String.valueOf(v.getDepth()));
      return m;
    };
    exporter.setVertexAttributeProvider(vertexAttributeProvider);

    // create provider of edge attributes
    ComponentAttributeProvider<DefaultWeightedEdge> edgeAttributeProvider =
        e -> {
          Map<String, String> m = new HashMap<>();
          m.put("name", e.toString());
          return m;
        };
    exporter.setEdgeAttributeProvider(edgeAttributeProvider);

    return exporter;
  }

  /**
   * Create importer
   */
  public static GraphImporter<WebVertex, DefaultWeightedEdge> createImporter() {
    // create vertex provider
    VertexProvider<WebVertex> vertexProvider = (url, attributes) -> {
      String baseUrl = attributes.get("baseUrl");
      int depth = Integer.valueOf(attributes.get("depth"));

      WebPage page = WebPage.newWebPage();
      page.setBaseUrl(baseUrl);

      return new WebVertex(url, "", WebPage.newWebPage(), depth);
    };

    // create edge provider
    EdgeProvider<WebVertex, DefaultWeightedEdge> edgeProvider = (from, to, label, attributes) -> new DefaultWeightedEdge();

    // create GraphML importer
    return new GraphMLImporter<>(vertexProvider, edgeProvider);
  }

}
