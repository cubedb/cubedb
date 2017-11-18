package org.cubedb.core;

import static org.junit.Assert.assertTrue;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.cubedb.core.beans.Filter;
import org.cubedb.utils.TestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class MultiCubeConcurrencyTest {
  public static final Logger log = LoggerFactory.getLogger(MultiCubeConcurrencyTest.class);

  @Test
  public void testParallelGetAndSave() throws IOException, InterruptedException {

    Path savePath = Files.createTempDirectory("savePath");
    MultiCubeImpl c = new MultiCubeImpl(savePath.toString());
    int numCubes = Runtime.getRuntime().availableProcessors();
    int numPartitions = Runtime.getRuntime().availableProcessors();
    for (int cubeId = 0; cubeId < numCubes; cubeId++)
      for (int p = 0; p < numPartitions; p++)
        c.insert(TestUtils.genMultiColumnData("c_" + cubeId, "p_" + p, "f_", 6, 6));
    Saver saver = new Saver(savePath.toString(), c);
    Thread saverThread = new Thread(saver);
    final long t0 = System.nanoTime();
    saverThread.start();
    final long t1 = System.nanoTime();
    c.get("c_0", numPartitions, new ArrayList<Filter>());
    final long t2 = System.nanoTime();
    saverThread.join();
    final long t3 = System.nanoTime();
    final long saveTime = saver.totalTimeNs;
    final long getTime = t2 - t1;
    final long allTime = t3 - t0;
    log.info(
        "Took {}mks to save, {}mks to get, {}mks alltogether in parallel",
        saveTime / 1000,
        getTime / 1000,
        allTime / 1000);
    assertTrue(allTime < getTime + saveTime);
    assertTrue(Long.max(getTime, saveTime) * 100 / allTime > 80);
    FileUtils.deleteDirectory(savePath.toFile());
  }

  @Test
  public void testParallelSaveAndSave() throws IOException, InterruptedException {
    Path savePath = Files.createTempDirectory("savePath");
    MultiCubeImpl c = new MultiCubeImpl(savePath.toString());
    int numCubes = Runtime.getRuntime().availableProcessors();
    int numPartitions = Runtime.getRuntime().availableProcessors();
    for (int cubeId = 0; cubeId < numCubes; cubeId++)
      for (int p = 0; p < numPartitions; p++)
        c.insert(TestUtils.genMultiColumnData("c_" + cubeId, "p_" + p, "f_", 6, 6));
    final Saver s1 = new Saver(savePath.toString(), c);
    Thread st1 = new Thread(s1);
    final Saver s2 = new Saver(savePath.toString(), c);
    Thread st2 = new Thread(s2);
    final long t0 = System.nanoTime();
    st1.start();
    st2.start();
    st1.join();
    st2.join();
    final long t1 = System.nanoTime();
    final long saveTime1 = s1.totalTimeNs;
    final long saveTime2 = s2.totalTimeNs;
    final long allTime = t1 - t0;
    log.info(
        "Took {}mks to save1, {}mks to save2, {}mks alltogether in parallel",
        saveTime1 / 1000,
        saveTime2 / 1000,
        allTime / 1000);
    assertTrue(allTime < saveTime1 + saveTime2);
    FileUtils.deleteDirectory(savePath.toFile());
  }

  protected static class Saver implements Runnable {
    public long totalTimeNs;
    final String savePath;
    final MultiCube c;

    public Saver(String savePath, MultiCube c) {
      super();
      this.savePath = savePath;
      this.c = c;
    }

    @Override
    public void run() {
      try {
        log.info("Saving...");
        long t0 = System.nanoTime();
        c.save(savePath);
        totalTimeNs = System.nanoTime() - t0;
        log.info("Finished. Saving took {}ms", totalTimeNs / 1000000);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}
