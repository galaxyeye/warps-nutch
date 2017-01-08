package org.apache.nutch.graph.io;

import org.apache.gora.util.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.nutch.graph.io.WebGraphWritable.OptimizeMode.*;

/**
 * Created by vincent on 16-12-30.
 * Copyright @ 2013-2017 Warpspeed Information. All rights reserved
 */
public class WebGraphWritable implements Writable {
  public enum OptimizeMode {
    NONE('n'), IGNORE_SOURCE('s'), IGNORE_TARGET('t'), IGNORE_EDGE('e');
    private char mode;

    OptimizeMode(char mode) {
      this.mode = mode;
    }

    public char value() { return mode; }

    public static OptimizeMode of(char b) {
      switch (b) {
        case 'n':
          return NONE;
        case 's':
          return IGNORE_SOURCE;
        case 't':
          return IGNORE_TARGET;
        case 'e':
          return IGNORE_EDGE;
      }
      return NONE;
    }
  }

  private Configuration conf;
  private OptimizeMode optimizeMode = OptimizeMode.NONE;
  private WebGraph graph;

  public WebGraphWritable() {
  }

  public WebGraphWritable(WebGraph graph, OptimizeMode optimizeMode, Configuration conf) {
    this.conf = conf;
    this.optimizeMode = optimizeMode;
    this.graph = graph;
  }

  public WebGraphWritable(WebGraph graph, Configuration conf) {
    this.conf = conf;
    this.graph = graph;
  }

  public WebGraphWritable reset(WebGraph graph) {
    this.graph = graph;
    return this;
  }

  public OptimizeMode getOptimizeMode() { return optimizeMode; }

  public WebGraph get() { return graph; }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeChar(optimizeMode.value());
    output.writeInt(graph.edgeSet().size());

    for (WebEdge edge : graph.edgeSet()) {
      output.writeBoolean(edge.isLoop());

      if (optimizeMode != IGNORE_EDGE) {
        Text.writeString(output, edge.getAnchor());
        output.writeDouble(graph.getEdgeWeight(edge));
      }

      if (optimizeMode != IGNORE_SOURCE) {
        IOUtils.serialize(conf, output, new WebVertexWritable(edge.getSource(), conf), WebVertexWritable.class);
      }
      if (optimizeMode != IGNORE_TARGET) {
        IOUtils.serialize(conf, output, new WebVertexWritable(edge.getTarget(), conf), WebVertexWritable.class);
      }
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    graph = new WebGraph();

    optimizeMode = OptimizeMode.of(input.readChar());
    int edgeSize = input.readInt();

    String anchor = "";
    double weight = 0;

    for (int i = 0; i < edgeSize; ++i) {
      boolean isLoop = input.readBoolean();

      if (optimizeMode != IGNORE_EDGE) {
        anchor = Text.readString(input);
        weight = input.readDouble();
      }

      WebVertexWritable source = new WebVertexWritable();
      WebVertexWritable target = new WebVertexWritable();
      if (optimizeMode != IGNORE_SOURCE) {
        source = IOUtils.deserialize(conf, input, null, WebVertexWritable.class);
        if (isLoop) target = source;
      }
      if (optimizeMode != IGNORE_TARGET) {
        target = IOUtils.deserialize(conf, input, null, WebVertexWritable.class);
        if (isLoop) source = target;
      }

      graph.addEdgeLenient(source.get(), target.get(), weight).setAnchor(anchor);
    }
  }
}
