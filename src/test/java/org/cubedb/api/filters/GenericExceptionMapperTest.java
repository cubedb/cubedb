package org.cubedb.api.filters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Map;

/** Created by krash on 28.06.17. */
public class GenericExceptionMapperTest {

  @SuppressWarnings("unchecked")
  @Test
  public void toResponse() throws Exception {

    GenericExceptionMapper mapper = new GenericExceptionMapper();
    Response response = mapper.toResponse(new Exception("HELLO"));
    assertEquals(Response.Status.INTERNAL_SERVER_ERROR, response.getStatusInfo());
    assertTrue(response.getEntity() instanceof Map);
    Map<String, String> map = (Map<String, String>) response.getEntity();
    assertTrue(map.containsKey("message"));
    assertTrue(map.containsKey("causedBy"));
    assertTrue(map.containsKey("trace"));
  }
}
