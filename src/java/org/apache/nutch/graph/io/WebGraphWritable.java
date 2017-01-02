package org.apache.nutch.graph.io;

import org.apache.gora.util.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vincent on 16-12-30.
 */
public class WebGraphWritable implements Writable {
  private Configuration conf;
  private WebGraph graph;

  public WebGraphWritable() {}

  public WebGraphWritable(WebGraph graph, Configuration conf) {
    this.conf = conf;
    this.graph = graph;
  }

  public WebGraph get() { return graph; }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(graph.edgeSet().size());
    for (WebEdge edge : graph.edgeSet()) {
      IOUtils.serialize(conf, output, new WebVertexWritable(edge.getSource(), conf), WebVertexWritable.class);
      IOUtils.serialize(conf, output, new WebVertexWritable(edge.getTarget(), conf), WebVertexWritable.class);
      output.writeDouble(graph.getEdgeWeight(edge));
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    graph = new WebGraph();

    int edgeSize = input.readInt();
    for (int i = 0; i < edgeSize; ++i) {
      WebVertexWritable source = IOUtils.deserialize(conf, input, null, WebVertexWritable.class);
      WebVertexWritable target = IOUtils.deserialize(conf, input, null, WebVertexWritable.class);
      double weight = input.readDouble();

      graph.addEdgeLenient(source.get(), target.get(), weight);
    }
  }
}
