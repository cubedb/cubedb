package org.cubedb.api.filters;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {

  @Override
  public Response toResponse(Throwable ex) {

    Map<String, String> response = new HashMap<String, String>();
    response.put("message", ex.getMessage());

    response.put("trace", throwableToString(ex));
    response.put("causedBy", throwableToString(ex.getCause()));

    return Response.status(Status.INTERNAL_SERVER_ERROR)
      .entity(response)
      .type(MediaType.APPLICATION_JSON)
      .build();
  }

  public static String throwableToString(Throwable e) {
    if (e == null) return null;
    StringWriter errorStackTrace = new StringWriter();
    e.printStackTrace(new PrintWriter(errorStackTrace));
    return errorStackTrace.toString();
  }
}
