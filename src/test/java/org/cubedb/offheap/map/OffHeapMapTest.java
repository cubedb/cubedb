package org.cubedb.offheap.map;

import static org.junit.Assert.*;

import org.junit.Test;

public class OffHeapMapTest {

  @Test
  public void test() {
    OffHeapMap map = new OffHeapMap(10);
    byte[] t1="test".getBytes();
    assertNull(map.get(t1));
    map.put(t1, 1);
    assertEquals(1, map.get(t1).intValue());
    map.put(t1, 2);
    assertEquals(2, map.get(t1).intValue());
    for(int i=0;i<1000;i++)
    {
      byte[] t = ("test"+i).getBytes();
      assertNull(map.get(t));
      map.put(t, 1);
      assertEquals(1, map.get(t).intValue());
      map.put(t, 2);
      assertEquals(2, map.get(t).intValue());
    }
    
    for(int i=0;i<1000;i++)
    {
      byte[] t = ("test"+i).getBytes();
      assertNotNull(map.get(t));
      map.put(t, 1);
      assertEquals(1, map.get(t).intValue());
      map.put(t, 2);
      assertEquals(2, map.get(t).intValue());
    }
  }

}
