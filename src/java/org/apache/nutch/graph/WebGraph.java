package org.apache.nutch.graph;

import org.apache.nutch.persist.WebPage;
import org.jgrapht.ext.*;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedWeightedPseudograph;

import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vincent on 16-12-21.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class WebGraph extends DirectedWeightedPseudograph<WebVertex, WebEdge> {

  public final static WebGraph EMPTY_WEB_GRAPH = new WebGraph();

  private WebVertex focus;

  public WebGraph() { super(WebEdge.class); }

  public WebEdge addEdgeLenient(WebVertex sourceVertex, WebVertex targetVertex, double weight) {
    addVertex(sourceVertex);
    addVertex(targetVertex);
    WebEdge edge = addEdge(sourceVertex, targetVertex);
    setEdgeWeight(edge, weight);
    return edge;
  }

  public WebEdge addEdgeLenient(WebVertex sourceVertex, WebVertex targetVertex) {
    return addEdgeLenient(sourceVertex, targetVertex, 0.0);
  }

  public static WebGraph of(WebEdge edge, WebGraph graph) {
    WebGraph subgraph = new WebGraph();
    subgraph.addEdgeLenient(graph.getEdgeSource(edge), graph.getEdgeTarget(edge), graph.getEdgeWeight(edge));
    return subgraph;
  }

  public WebVertex getFocus() { return focus; }

  public void setFocus(WebVertex focus) {
    if (!containsVertex(focus)) {
      addVertex(focus);
    }

    this.focus = focus;
  }

  public WebEdge firstEdge() {
    return edgeSet().iterator().next();
  }

  /**
   * Create exporter
   */
  public static GraphExporter<WebVertex, WebEdge> createExporter() {
    // create GraphML exporter
    GraphMLExporter<WebVertex, WebEdge> exporter = new GraphMLExporter<>(WebVertex::getUrl, null, DefaultEdge::toString, null);

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
      m.put("depth", String.valueOf(v.getWebPage().getDepth()));
      return m;
    };
    exporter.setVertexAttributeProvider(vertexAttributeProvider);

    // create provider of edge attributes
    ComponentAttributeProvider<WebEdge> edgeAttributeProvider =
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
  public static GraphImporter<WebVertex, WebEdge> createImporter() {
    // create vertex provider
    VertexProvider<WebVertex> vertexProvider = (url, attributes) -> {
      String baseUrl = attributes.get("baseUrl");
      int depth = Integer.valueOf(attributes.get("depth"));

      WebPage page = WebPage.newWebPage();
      page.setBaseUrl(baseUrl);
      page.setDepth(depth);

      return new WebVertex(url, WebPage.newWebPage());
    };

    // create edge provider
    EdgeProvider<WebVertex, WebEdge> edgeProvider = (from, to, label, attributes) -> new WebEdge();

    // create GraphML importer
    return new GraphMLImporter<>(vertexProvider, edgeProvider);
  }

  @Override
  public String toString() {
    GraphExporter<WebVertex, WebEdge> exporter = WebGraph.createExporter();
    Writer writer = new StringWriter();
    try {
      exporter.exportGraph(this, writer);
    } catch (ExportException e) {
      return e.toString();
    }
    return writer.toString();
  }
}
