package org.apache.nutch.persist.graph;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by vincent on 16-12-21.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class Graph implements Writable, Configurable {
  private Configuration conf;
  private Map<String, Vertex> vertices = new TreeMap<>();
  private List<Edge> edges = new ArrayList<>();
  private GraphMode graphMode;

  public Graph(Configuration conf) {
    this.conf = conf;
  }

  public void reset() {
    vertices.clear();
    edges.clear();
  }

  public void addVertex(Vertex vertex) { vertices.put(vertex.getUrl(), vertex); }

  public Vertex getVertex() { return vertices.values().iterator().next(); }

  public Vertex getVertex(String url) { return vertices.get(url); }

  public Collection<Vertex> getVertices() { return vertices.values(); }

  public Collection<Vertex> getStartVertices() {
    return edges.stream().map(Edge::getV1).collect(Collectors.toList());
  }

  public Collection<Vertex> getEndVertices() {
    return edges.stream().map(Edge::getV2).collect(Collectors.toList());
  }

  public void addEdge(Edge edge) {
    Vertex v1 = edge.getV1();
    Vertex v2 = edge.getV2();
    vertices.put(v1.getUrl(), v1);
    vertices.put(v2.getUrl(), v2);
    edges.add(edge);
  }

  public void addEdges(Collection<Edge> edges) {
    this.edges.addAll(edges);
  }

  public List<Edge> getEdges() { return edges; }

  public GraphMode getGraphMode() { return graphMode; }

  public int vertexCount() { return vertices.size(); }

  public int edgeCount() { return edges.size(); }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(edges.size());
    for (Edge edge : edges) {
      edge.write(output);
    }

    Text.writeString(output, graphMode.name());
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    int count = input.readInt();
    for (int i = 0; i < count; ++i) {
      Edge edge = new Edge();
      edge.readFields(input);
      addEdge(edge);
    }

    graphMode = GraphMode.valueOf(Text.readString(input));
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
