package org.apache.nutch.graph;

import org.apache.nutch.persist.WebPage;
import org.apache.nutch.util.StringUtil;
import org.jgrapht.VertexFactory;
import org.jgrapht.ext.ExportException;
import org.jgrapht.ext.GraphExporter;
import org.jgrapht.ext.GraphImporter;
import org.jgrapht.ext.ImportException;
import org.jgrapht.generate.CompleteGraphGenerator;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.graph.DirectedWeightedPseudograph;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Random;

/**
 * Created by vincent on 16-12-29.
 */
public class GraphDemo {
  // Number of vertices
  private static final int SIZE = 6;

  private static Random generator = new Random(17);

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
    DirectedPseudograph<WebVertex, WebEdge> graph1 = new DirectedWeightedPseudograph<>(WebEdge.class);
    CompleteGraphGenerator<WebVertex, WebEdge> completeGenerator = new CompleteGraphGenerator<>(SIZE);
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
    for (WebEdge e : graph1.edgeSet()) {
      graph1.setEdgeWeight(e, generator.nextInt(100));
    }

    // now export and import back again
    try {
      // export as string
      System.out.println("-- Exporting graph as GraphML");
      GraphExporter<WebVertex, WebEdge> exporter = WebGraph.createExporter();
      Writer writer = new StringWriter();
      exporter.exportGraph(graph1, writer);
      String graph1AsGraphML = writer.toString();

      // display
      System.out.println(graph1AsGraphML);

      // import it back
      System.out.println("-- Importing graph back from GraphML");
      org.jgrapht.Graph<WebVertex, WebEdge> graph2 = new DirectedWeightedPseudograph<>(WebEdge.class);
      GraphImporter<WebVertex, WebEdge> importer = WebGraph.createImporter();
      importer.importGraph(graph2, new StringReader(graph1AsGraphML));
    } catch (ExportException | ImportException e) {
      System.err.println("Error: " + StringUtil.stringifyException(e));
      System.exit(-1);
    }
  }
}
