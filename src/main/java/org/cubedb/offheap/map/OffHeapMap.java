package org.cubedb.offheap.map;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.cubedb.core.KeyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffHeapMap implements KeyMap {
  int[][] buckets;
  public final int BUFFER_SIZE = 5 * 1024;
  public int candidatesMaxSize = 3;
  private ByteBuffer buf;
  private int currentPos = 0;
  int elementCount = 0;
  private static final Logger log = LoggerFactory.getLogger(OffHeapMap.class);
  


  public OffHeapMap(int initialSize) {
    buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
    rebucket(initialSize);
  }

  protected byte[] getKey(int pos) {
    int size = buf.get(pos);
    byte[] out = new byte[size];
    buf.position(pos + 1);
    buf.get(out, 0, size);
    return out;
  }

  protected int getValue(int pos) {
    int size = buf.get(pos);
    return buf.getInt(pos + 1 + size);
  }

  protected void putValue(int pos, int val) {
    int size = buf.get(pos);
    buf.putInt(pos + 1 + size, val);
  }

  protected int appendEntry(byte[] key, int val) {
    if(currentPos+1+key.length + Integer.BYTES>buf.capacity())
    {
      //log.debug("Resizing direct buffer");
      resize(buf.capacity()*2);
    }
    buf.put(currentPos, (byte) key.length);
    buf.position(currentPos + 1);
    buf.put(key, 0, key.length);
    buf.putInt(currentPos + 1 + key.length, val);
    int prevPos = currentPos;
    currentPos += key.length + 1 + Integer.BYTES;
    elementCount++;
    return prevPos;
  }

  protected byte[] getKeyIfSameSize(int pos, int desiredSize) {
    int size = buf.get(pos);
    if (size != desiredSize)
      return null;
    byte[] out = new byte[size];
    buf.position(pos + 1);
    buf.get(out, 0, size);
    return out;
  }

  protected int[] getCandidates(byte[] b) {
    final int hashCode = Arrays.hashCode(b);
    int candidatePos = hashCode % buckets.length;
    if(candidatePos<0) candidatePos=-candidatePos;
    if(candidatePos<0)
    {
      log.error("Just look at this amazing shit!");
      log.error("{}, hashCode is {}, candidatePos is {}, number of buckets is {}", Arrays.toString(b), hashCode, candidatePos, buckets.length);
    }
    int[] candidates = buckets[candidatePos];
    if (candidates == null) {
      candidates = new int[candidatesMaxSize];
      buckets[candidatePos] = candidates;
      Arrays.fill(candidates, -1);
    }
    return candidates;
  }

  @Override
  public Integer get(byte[] b) {
    int[] candidates = getCandidates(b);
    int i = 0;
    while (i < candidates.length && candidates[i] >= 0) {
      int pos = candidates[i];
      byte[] candidate = getKeyIfSameSize(pos, b.length);
      if (Arrays.equals(b, candidate))
        return getValue(pos);
      i++;
    }
    return null;
    /**/
  }

  @Override
  public synchronized void put(byte[] k, int v) {
    int[] candidates = getCandidates(k);
    int i = 0;
    while (i < candidates.length && candidates[i] >= 0) {
      int pos = candidates[i];
      byte[] candidate = getKeyIfSameSize(pos, k.length);
      if (Arrays.equals(k, candidate)) {
        putValue(pos, v);
        return;
      }
      i++;
    }
    int newPos = appendEntry(k, v);
    if (i == candidates.length)
      rebucket(this.elementCount * 2);
    else
      candidates[i] = newPos;

  }

  private void rebucket(int newSize) {
    candidatesMaxSize+=2;
    this.buckets = new int[Integer.max(newSize, 10)][];
    //log.debug("Rebucketing to {} buckets {} each", this.buckets.length, candidatesMaxSize);
    int pos = 0;
    while (pos < currentPos) {
      byte[] k = getKey(pos);
      int[] candidates = getCandidates(k);
      int i = 0;
      while (candidates[i] >= 0) {
        i++;
        if (i >= candidates.length) {
          log.debug("Rebucketing to size {}, again, because there are more then {} values in a bucket", newSize + 1, i);
          rebucket(newSize + 1);
          log.debug("Utilization of buckets is now {}%", getUtilization());
          return;
        }
      }
      candidates[i] = pos;
      pos += 1 + Integer.BYTES + k.length;
    }
  }

  
  private int getUtilization()
  {
    int numUsed=0;
    for(int i=0;i<this.buckets.length;i++)
      if(this.buckets[i]!=null)
      {
        numUsed++;
      }
   return numUsed*100/this.buckets.length;
  }
  private void resize(int newSize) {
    ByteBuffer newBuf = ByteBuffer.allocateDirect(newSize);
    buf.position(0);
    for (int i = 0; i < buf.capacity(); i++)
      newBuf.put(buf.get());
    this.buf.clear();
    this.buf = newBuf;
    
  }


  @Override
  public int size() {
    return elementCount;
  }

  @Override
  public Stream<Entry<byte[], Integer>> entrySet() {
    final Iterator<Entry<byte[], Integer>> entryIterator = new Iterator<Entry<byte[], Integer>>() {
      private int pos = 0;

      @Override
      public boolean hasNext() {
        return pos < currentPos;
      }

      @Override
      public Entry<byte[], Integer> next() {
        byte[] k = getKey(pos);
        int v = getValue(pos);
        Entry<byte[], Integer> e = new TinyEntry<byte[], Integer>(k, v);
        pos += 1 + Integer.BYTES + k.length;
        return e;
      }

    };
    return StreamSupport
        .stream(Spliterators.spliterator(entryIterator, size(), Spliterator.ORDERED), false);
  }



  final class TinyEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private V value;

    public TinyEntry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      V old = this.value;
      this.value = value;
      return old;
    }
  }
}
