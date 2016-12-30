package org.apache.nutch.graph;

import org.apache.nutch.persist.WebPage;
import org.apache.nutch.util.StringUtil;
import org.jgrapht.VertexFactory;
import org.jgrapht.ext.*;
import org.jgrapht.generate.CompleteGraphGenerator;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.graph.DirectedWeightedPseudograph;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by vincent on 16-12-29.
 */
public class GraphDemo {
  // Number of vertices
  private static final int SIZE = 6;

  private static Random generator = new Random(17);

  /**
   * Create exporter
   */
  private static GraphExporter<WebVertex, DefaultWeightedEdge> createExporter() {
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
  private static GraphImporter<WebVertex, DefaultWeightedEdge> createImporter() {
    // create vertex provider
    VertexProvider<WebVertex> vertexProvider = (url, attributes) -> {
      String baseUrl = attributes.get("baseUrl");
      int depth = Integer.valueOf(attributes.get("depth"));

      WebPage page = WebPage.newWebPage();
      page.setBaseUrl(baseUrl);

      return new WebVertex(url, WebPage.newWebPage(), depth);
    };

    // create edge provider
    EdgeProvider<WebVertex, DefaultWeightedEdge> edgeProvider = (from, to, label, attributes) -> new DefaultWeightedEdge();

    // create GraphML importer
    return new GraphMLImporter<>(vertexProvider, edgeProvider);
  }

  /**
   * Main demo method
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    /*
     * Generate complete graph.
     *
     * Vertices have random colors and edges have random edge weights.
     */
    DirectedPseudograph<WebVertex, DefaultWeightedEdge> graph1 = new DirectedWeightedPseudograph<>(DefaultWeightedEdge.class);
    CompleteGraphGenerator<WebVertex, DefaultWeightedEdge> completeGenerator = new CompleteGraphGenerator<>(SIZE);
    VertexFactory<WebVertex> vFactory = new VertexFactory<WebVertex>() {
      private int sequence = 0;

      @Override
      public WebVertex createVertex() {
        ++sequence;

        String url = "http://t.tt/" + sequence;
        WebPage page = WebPage.newWebPage();
        page.setBaseUrl(url);
        int depth = sequence % 10;
        return new WebVertex(url, page, depth);
      }
    };
    System.out.println("-- Generating complete graph");
    completeGenerator.generateGraph(graph1, vFactory, null);

    // assign random weights
    for (DefaultWeightedEdge e : graph1.edgeSet()) {
      graph1.setEdgeWeight(e, generator.nextInt(100));
    }

    // now export and import back again
    try {
      // export as string
      System.out.println("-- Exporting graph as GraphML");
      GraphExporter<WebVertex, DefaultWeightedEdge> exporter = createExporter();
      Writer writer = new StringWriter();
      exporter.exportGraph(graph1, writer);
      String graph1AsGraphML = writer.toString();

      // display
      System.out.println(graph1AsGraphML);

      // import it back
      System.out.println("-- Importing graph back from GraphML");
      org.jgrapht.Graph<WebVertex, DefaultWeightedEdge> graph2 = new DirectedWeightedPseudograph<>(DefaultWeightedEdge.class);
      GraphImporter<WebVertex, DefaultWeightedEdge> importer = createImporter();
      importer.importGraph(graph2, new StringReader(graph1AsGraphML));
    } catch (ExportException | ImportException e) {
      System.err.println("Error: " + StringUtil.stringifyException(e));
      System.exit(-1);
    }
  }
}
