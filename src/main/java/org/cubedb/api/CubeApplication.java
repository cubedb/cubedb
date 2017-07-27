package org.cubedb.api;

import org.cubedb.api.ext.JsonIteratorConverter;
import org.cubedb.api.filters.AccessOriginFilter;
import org.cubedb.api.filters.GenericExceptionMapper;
import org.cubedb.api.resources.CubeResource;
import org.cubedb.core.Constants;
import org.cubedb.core.MultiCube;
import org.cubedb.core.MultiCubeImpl;

import com.esotericsoftware.minlog.Log;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;

import java.io.File;
import java.net.URI;
import javax.ws.rs.core.UriBuilder;

public class CubeApplication extends ResourceConfig {

  private MultiCube cube;

  public CubeApplication(ServerConfiguration config, MultiCube cube) {
    this.cube = cube;
    registerInstances(new CubeResource(this.cube));
  }

  public static void main(String... args) throws Exception {

    ServerConfiguration config = new ServerConfiguration();
    config.port = Integer.parseInt(args[0]);
    config.path = args[1];
    config.defaultPartitionName = Constants.DEFAULT_PARTITION_NAME;
    runWithConfig(config);
  }

  protected static void registerStuff(ResourceConfig rConfig) {

    rConfig.register(AccessOriginFilter.class);
    /*
     * rConfig.register(ErrorMapper.class);
     * rConfig.register(PreFilter.class);
     * rConfig.register(PostFilter.class);
     */
    rConfig.register(GenericExceptionMapper.class);
    rConfig.register(org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpContainerProvider.class);
    EncodingFilter.enableFor(rConfig, GZipEncoder.class);
    rConfig.register(JsonIteratorConverter.class);
  }

  public static void runWithConfig(ServerConfiguration config) throws Exception {
    URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(config.port).build();
    // ResourceConfig rConfig = new
    // ResourceConfig(QueryResource.class).register;

    MultiCube cube = new MultiCubeImpl(new File(config.path).getAbsolutePath());
    cube.load(cube.getPath());
    CubeApplication rConfig = new CubeApplication(config, cube);
    registerStuff(rConfig);
    HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, rConfig);
    Log.info("Starting server");
    server.start();
    Thread.currentThread().join();
    Log.info("Shutting down");
  }
}
