package org.cubedb.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.cubedb.utils.CubeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CubeUtilsTest {
  public static final Logger log = LoggerFactory.getLogger(CubeUtilsTest.class);

  protected <T> void ensureSameLengthAndNoDuplicates(List<T> in, int numParts) {
    List<List<T>> out = CubeUtils.partitionList(in, numParts);
    int totalSize = out.stream().mapToInt(List::size).sum();
    int minSize = out.stream().mapToInt(List::size).min().orElse(0);
    int maxSize = out.stream().mapToInt(List::size).max().orElse(0);
    assertTrue(maxSize - minSize <= 1);
    //log.info("{}", out.stream().mapToInt(List::size).toArray());
    assertEquals(in.size(), totalSize);
    assertEquals(in, out.stream().flatMap(List::stream).collect(Collectors.toList()));
  }

  @Test
  public void test() {
    int maxParts = 96;
    int maxLength = 480;
    for (int l = 0; l < maxLength; l++) {
      List<Integer> list = new ArrayList<Integer>(maxLength);
      for (int j = 0; j < l; j++) {
        list.add(j);
      }
      for (int p = 1; p < maxParts; p++) {
        //log.info("l: {}, p: {}", l, p);
        ensureSameLengthAndNoDuplicates(list, p);
      }
    }
  }

  @Test
  public void test12_8() {
    int maxParts = Runtime.getRuntime().availableProcessors();
    int maxLength = 12;
    List<Integer> list = new ArrayList<Integer>(maxLength);
    for (int j = 0; j < maxLength; j++) {
      list.add(j);
    }
    List<List<Integer>> out = CubeUtils.partitionList(list, maxParts);
    log.info("Out is {}", out);
    assertEquals(maxParts, out.size());
  }

  @Test
  public void testCutSuffix() {
    assertArrayEquals(
        new short[] {0, 0, 1, 2, 3}, CubeUtils.cutZeroSuffix(new short[] {0, 0, 1, 2, 3, 0, 0, 0}));
    assertArrayEquals(
        new short[] {1, 2, 3}, CubeUtils.cutZeroSuffix(new short[] {1, 2, 3, 0, 0, 0}));
    assertArrayEquals(new short[] {1, 2, 3}, CubeUtils.cutZeroSuffix(new short[] {1, 2, 3}));
    assertArrayEquals(new short[] {}, CubeUtils.cutZeroSuffix(new short[] {0, 0, 0, 0, 0}));
    assertArrayEquals(new short[] {}, CubeUtils.cutZeroSuffix(new short[] {}));
  }
}
