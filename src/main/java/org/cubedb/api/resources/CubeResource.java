package org.cubedb.api.resources;


import org.cubedb.api.utils.APIResponse;
import org.cubedb.core.MultiCube;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.utils.CubeUtils;

import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

@Singleton
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
public class CubeResource {
  public static final Logger log = LoggerFactory.getLogger(CubeResource.class);

  @Context // injected response proxy supporting multiple threads
  private HttpServletResponse response;

  protected MultiCube cube;

  public CubeResource(MultiCube cube) {
    this.cube = cube;
  }

  @GET
  @Path("/{cubeName}/last/{range}")
  public APIResponse<Map<String, Map<String, Map<String, Long>>>> get(
      @PathParam("cubeName") String cubeName,
      @PathParam("range") int range,
      @Context UriInfo info) {
    final long startTime = System.currentTimeMillis();
    final MultivaluedMap<String, String> filterCriterias = info.getQueryParameters();

    if (!cube.hasCube(cubeName)) {
      log.warn("Could not find cube {}", cubeName);
      throw new NotFoundException(String.format("Could not find cube %s", cubeName));
    }
    Map<GroupedSearchResultRow, Long> result =
        cube.get(cubeName, range, buildFilters(filterCriterias));
    return new APIResponse<>(CubeUtils.searchResultsToMap(result), info, startTime);
  }

  @GET
  @Path("/{cubeName}/last/{range}/group_by/{groupBy}")
  public APIResponse<Map<String, Map<String, Map<String, Map<String, Long>>>>> get(
      @PathParam("cubeName") String cubeName,
      @PathParam("range") int range,
      @PathParam("groupBy") String groupBy,
      @Context UriInfo info) {
    final long startTime = System.currentTimeMillis();
    final MultivaluedMap<String, String> filterCriterias = info.getQueryParameters();

    if (!cube.hasCube(cubeName)) {
      log.warn("Could not find cube {}", cubeName);
      throw new NotFoundException(String.format("Could not find cube %s", cubeName));
    }

    Map<GroupedSearchResultRow, Long> result =
        cube.get(cubeName, range, buildFilters(filterCriterias), groupBy);
    final long timeBeforeGrouping = System.currentTimeMillis();
    Map<String, Map<String, Map<String, Map<String, Long>>>> groups =
        CubeUtils.searchResultsToGroupedMap(result);
    final long timeAfterGrouping = System.currentTimeMillis();
    log.debug("Grouping took {}ms" + (timeAfterGrouping - timeBeforeGrouping));
    return new APIResponse<>(groups, info, startTime);
  }

  private List<Filter> buildFilters(MultivaluedMap<String, String> filterCriterias) {
    List<Filter> filters = new ArrayList<Filter>();
    for (Entry<String, List<String>> filterE : filterCriterias.entrySet()) {
      String[] values = filterE.getValue().toArray(new String[0]);
      Filter f = new Filter();
      f.setField(filterE.getKey());
      f.setValues(values);
      filters.add(f);
    }
    return filters;
  }

  @POST
  @Path("/insert")
  public APIResponse<Map<String, Integer>> insert(List<DataRow> rows, @Context UriInfo info) {
    long startTs = System.currentTimeMillis();
    cube.insert(rows);
    log.info("Inserted {} rows", rows.size());
    return new APIResponse<>(ImmutableMap.of("numInsertedRows", rows.size()), info, startTs);
  }

  @DELETE
  @Path("/keep/last/{numPartitions}")
  public APIResponse<Map<String, Integer>> keepLastN(
      @PathParam("numPartitions") Integer numPartitions, @Context UriInfo info) {
    long startTs = System.currentTimeMillis();
    int numDeletedPartitions = cube.deleteCube(numPartitions);
    int numOptimizedPartitions = cube.optimize();
    long t0 = System.currentTimeMillis();
    System.gc();
    log.debug("GC took {}ms", System.currentTimeMillis() - t0);
    return new APIResponse<>(
        ImmutableMap.of(
            "numDeletedPartitions", numDeletedPartitions,
            "numOptimizedPartitions", numOptimizedPartitions,
            "gcTimeMs", (int) (System.currentTimeMillis() - t0)),
        info,
        startTs);
  }

  @DELETE
  @Path("/{cubeName}")
  public APIResponse<Map<String, Integer>> deleteCube(
      @PathParam("cubeName") String cubeName, @Context UriInfo info) {
    if (!cube.hasCube(cubeName)) {
      log.warn("Could not find cube {}", cubeName);
      throw new NotFoundException(String.format("Could not find cube %s", cubeName));
    }

    long startTs = System.currentTimeMillis();
    int numDeletedPartitions = cube.deleteCube(cubeName, 0);
    int numOptimizedPartitions = cube.optimize();
    long t0 = System.currentTimeMillis();
    System.gc();
    log.debug("GC took {}ms", System.currentTimeMillis() - t0);
    return new APIResponse<>(
        ImmutableMap.of(
            "numDeletedPartitions", numDeletedPartitions,
            "numOptimizedPartitions", numOptimizedPartitions,
            "gcTimeMs", (int) (System.currentTimeMillis() - t0)),
        info,
        startTs);
  }

  @DELETE
  @Path("/all/from/{fromPartition}/to/{toPartition}")
  public APIResponse<Map<String, Integer>> deletePartitions(
      @PathParam("fromPartition") String fromPartition,
      @PathParam("toPartition") String toPartition,
      @Context UriInfo info) {
    long startTs = System.currentTimeMillis();
    int numDeletedPartitions = cube.deleteCube(fromPartition, toPartition);
    int numOptimizedPartitions = cube.optimize();
    long t0 = System.currentTimeMillis();
    System.gc();
    log.debug("GC took {}ms", System.currentTimeMillis() - t0);
    return new APIResponse<>(
        ImmutableMap.of(
            "numDeletedPartitions", numDeletedPartitions,
            "numOptimizedPartitions", numOptimizedPartitions,
            "gcTimeMs", (int) (System.currentTimeMillis() - t0)),
        info,
        startTs);
  }

  @DELETE
  @Path("/{cubeName}/from/{fromPartition}/to/{toPartition}")
  public APIResponse<Map<String, Integer>> deletePartitionsOfCube(
      @PathParam("cubeName") String cubeName,
      @PathParam("fromPartition") String fromPartition,
      @PathParam("toPartition") String toPartition,
      @Context UriInfo info) {
    if (!cube.hasCube(cubeName)) {
      log.warn("Could not find cube {}", cubeName);
      throw new NotFoundException(String.format("Could not find cube %s", cubeName));
    }

    long startTs = System.currentTimeMillis();
    int numDeletedPartitions = cube.deleteCube(cubeName, fromPartition, toPartition);
    int numOptimizedPartitions = cube.optimize();
    long t0 = System.currentTimeMillis();
    System.gc();
    log.debug("GC took {}ms", System.currentTimeMillis() - t0);
    return new APIResponse<>(
        ImmutableMap.of(
            "numDeletedPartitions", numDeletedPartitions,
            "numOptimizedPartitions", numOptimizedPartitions,
            "gcTimeMs", (int) (System.currentTimeMillis() - t0)),
        info,
        startTs);
  }

  @POST
  @Path("/save")
  public APIResponse<Map<String, String>> save(@Context UriInfo info) throws IOException {
    final long startTs = System.currentTimeMillis();
    log.info("Saving to {}", cube.getPath());
    cube.save(cube.getPath());
    log.info("Saving finished");
    return new APIResponse<>(ImmutableMap.of("savePath", cube.getPath()), info, startTs);
  }

  @POST
  @Path("/saveJSON")
  public APIResponse<Map<String, String>> dump(@Context UriInfo info) throws IOException {
    final long startTs = System.currentTimeMillis();
    String path = cube.getPath() + "/json";
    log.info("Saving to {}", path);
    cube.saveAsJson(path);
    log.info("Saving finished");
    return new APIResponse<>(ImmutableMap.of("savePath", path), info, startTs);
  }


  @GET
  @Path("/stats/{cubeName}")
  public APIResponse<Map<String, Object>> getCubeStats(@PathParam("cubeName") String cubeName,
                                                       @Context UriInfo info) throws IOException {
    final long startTs = System.currentTimeMillis();
    if (!cube.hasCube(cubeName)) {
      log.warn("Could not find cube {}", cubeName);
      throw new NotFoundException(String.format("Could not find cube %s", cubeName));
    }
    return new APIResponse<>(cube.getCube(cubeName).getStats(), info, startTs);
  }


  @GET
  @Path("/stats")
  public APIResponse<Map<String, Object>> getStats(@Context UriInfo info) {
    long startTs = System.currentTimeMillis();
    return new APIResponse<>(cube.getStats(), info, startTs);
  }
}
