package org.cubedb.api.resources;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Singleton
@Path("/")
public class IndexResource {

  @GET
  @Produces(MediaType.TEXT_HTML)
  public String index() {
    return "<html>"
        + "<h1>It works!</h1>"
        + "<p>For server stats navigate to <a href=\"/v1/stats/\">/v1/stats/</a></p>"
        + "</html>";
  }
}
