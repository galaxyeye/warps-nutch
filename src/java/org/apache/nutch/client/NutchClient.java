package org.apache.nutch.client;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.local.model.ServerInstance;
import org.apache.nutch.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class NutchClient {

  public static final Logger LOG = LoggerFactory.getLogger(NutchClient.class);

  private final Configuration conf;
  private final String host;
  private final int port;
  private final Client client;
  private WebResource nutchResource;

  public NutchClient(Configuration conf) {
    this(conf,
        conf.get("nutch.master.hostname", "localhost"),
        conf.getInt("nutch.server.port", 8182));
  }

  public NutchClient(Configuration conf, String host, int port) {
    this.conf = conf;
    this.host = host;
    this.port = port;

    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
    client = Client.create(clientConfig);
    String baseUrl = "http://" + host + ":" + port;
    nutchResource = client.resource(baseUrl);

    LOG.debug("NutchClient created, baseUrl : " + baseUrl);
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public boolean available() {
    return NetUtil.testNetwork(host, port);
  }

  public void register(ServerInstance instance) {
    nutchResource.path("service/register").type(APPLICATION_JSON).post(instance);
  }

  public void unregister(ServerInstance instance) {
    nutchResource.path("service/unregister").type(APPLICATION_JSON).delete(instance);
  }

  public int acquirePort(ServerInstance.Type type) {
    return nutchResource.path("port/acquire")
        .queryParam("type", type.name())
        .get(Integer.class);
  }

  public void recyclePort(ServerInstance.Type type, int port) {
    nutchResource.path("port/recycle")
        .queryParam("type", type.name())
        .queryParam("port", String.valueOf(port))
        .put();
  }
}
