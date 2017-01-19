package org.apache.nutch.net.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.persist.local.model.ServerInstance;
import org.apache.nutch.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.apache.nutch.metadata.Nutch.*;

public class NutchClient {

  public static final Logger LOG = LoggerFactory.getLogger(NutchClient.class);

  private final Configuration conf;
  private final String host;
  private final int port;
  private final Client client;
  private WebResource nutchResource;

  public NutchClient(Configuration conf) {
    this(conf,
        conf.get(PARAM_NUTCH_MASTER_HOST, DEFAULT_MASTER_HOSTNAME),
        conf.getInt(PARAM_NUTCH_MASTER_PORT, DEFAULT_MASTER_PORT));
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

    LOG.info("NutchClient created, baseUrl : " + baseUrl);
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  /**
   * Check if the server is available
   * TODO : it seems this is useless
   * */
  public boolean available() {
    return NetUtil.testNetwork(host, port);
  }

  /**
   * Register this fetch server instance
   * */
  public void register(ServerInstance instance) {
    nutchResource.path("service/register").type(APPLICATION_JSON).post(instance);
  }

  /**
   * Unregister this fetch server instance
   * */
  public void unregister(ServerInstance instance) {
    nutchResource.path("service/unregister").type(APPLICATION_JSON).delete(instance);
  }

  /**
   * Acquire a available fetch server port
   * */
  public int acquirePort(ServerInstance.Type type) {
    return nutchResource.path("port/acquire")
        .queryParam("type", type.name())
        .get(Integer.class);
  }

  /**
   * Resycle a available fetch server port
   * */
  public void recyclePort(ServerInstance.Type type, int port) {
    nutchResource.path("port/recycle")
        .queryParam("type", type.name())
        .queryParam("port", String.valueOf(port))
        .put();
  }
}
