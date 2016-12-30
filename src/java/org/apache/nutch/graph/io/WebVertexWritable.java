package org.apache.nutch.graph.io;

import org.apache.gora.util.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vincent on 16-12-30.
 */
public class WebVertexWritable implements Writable {
  private Configuration conf;
  private WebVertex vertex;

  public WebVertexWritable(WebVertex vertex, Configuration conf) {
    this.conf = conf;
    this.vertex = vertex;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    Text.writeString(output, vertex.getUrl());
    IOUtils.serialize(conf, output, vertex.getWebPage().get(), GoraWebPage.class);
    output.writeInt(vertex.getDepth());
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    String url = Text.readString(input);
    WebPage page = WebPage.wrap(IOUtils.deserialize(conf, input, vertex.getWebPage().get(), GoraWebPage.class));
    int weight = input.readInt();

    this.vertex = new WebVertex(url, page, weight);
  }
}
