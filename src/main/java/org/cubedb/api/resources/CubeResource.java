package org.cubedb.api.resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.cubedb.api.utils.APIResponse;
import org.cubedb.core.CubeImpl;
import org.cubedb.core.MultiCube;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.SearchResultRow;
import org.cubedb.utils.CubeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriInfo;

import com.esotericsoftware.minlog.Log;


@Singleton
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
public class CubeResource {
	public static final Logger log = LoggerFactory.getLogger(CubeResource.class);
	@Context  //injected response proxy supporting multiple threads
	private HttpServletResponse response;
	
	protected MultiCube cube;

	public CubeResource(MultiCube cube) {
		this.cube = cube;
	}

	@GET
	@Path("/{cubeName}/last/{range}")
	public APIResponse<Map<String, Map<String, Map<String, Long>>>> get(@PathParam("cubeName") String cubeName, @PathParam("range") int range,
			@Context UriInfo info) {
	final long startTime = System.currentTimeMillis();
	final MultivaluedMap<String, String> filterCriterias = info.getQueryParameters();
	
		if(!this.cube.hasCube(cubeName)){
			log.warn("Could not find cube {}", cubeName);
			throw new NotFoundException(String.format("Could not find cube %s",cubeName)); 
		}
		List<Filter> filters = new ArrayList<Filter>();
		for (Entry<String, List<String>> filterE : filterCriterias.entrySet()) {
			String[] values = (String[]) filterE.getValue().toArray(new String[0]);
			Filter f = new Filter();
			f.setField(filterE.getKey());
			f.setValues(values);
			filters.add(f);
		}
		Map<SearchResultRow, Long> result = this.cube.get(cubeName, range, filters);
		//out.put("results", result);
		//out.put("request", request);
		
		//entity = new GenericEntity<Map<String,Object>>(out){};
		//response.setStatus(HttpServletResponse.SC_OK);
		return new APIResponse<Map<String,Map<String,Map<String,Long>>>>(CubeUtils.searchResultsToMap(result),info, startTime);
		/*return Response.status(Status.OK)
				.entity(entity)
				.build();*/
	
	}

	@POST
	@Path("/insert")
	public APIResponse<Integer> insert(List<DataRow> rows, @Context UriInfo info) {
		long startTs = System.currentTimeMillis();
		cube.insert(rows);
		return new APIResponse<Integer>(rows.size(), info, startTs);
	}
	
	@POST
	@Path("/keep/last/{numPartitions}")
	public APIResponse<Integer> keepLastN(@PathParam("numPartitions")  Integer numPartitions, @Context UriInfo info) {
		long startTs = System.currentTimeMillis();
		int numDeletedPartitions = cube.deleteCube(numPartitions);
		return new APIResponse<Integer>(numDeletedPartitions, info, startTs);
	}
	
	@POST
	@Path("/save")
	public APIResponse<Boolean> save(@Context UriInfo info) {
		long startTs = System.currentTimeMillis();
		log.info("Saving to {}", cube.getPath());
		//cube.insert(rows);
		cube.save(cube.getPath());
		log.info("Saving finished");
		return new APIResponse<Boolean>(true, info, startTs);
	}
	
	@POST
	@Path("/shutdown")
	public APIResponse<Boolean> shutdown(@Context UriInfo info) {
		long startTs = System.currentTimeMillis();
		//cube.insert(rows);
		return new APIResponse<Boolean>(true, info, startTs);
	}
	
	@GET
	@Path("/stats")
	public APIResponse<Map<String, Object>> getStats(@Context UriInfo info) {
		long startTs = System.currentTimeMillis();
		//cube.insert(rows);
		return new APIResponse<Map<String, Object>>(cube.getStats(), info, startTs);
	}
}
