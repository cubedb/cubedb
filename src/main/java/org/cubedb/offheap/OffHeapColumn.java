package org.cubedb.offheap;

import org.cubedb.core.Column;

public class OffHeapColumn extends MultiBuffer implements Column {

  public OffHeapColumn(int startPos) {
    super(startPos, Short.BYTES);
  }

  public OffHeapColumn() {
    this(0);
  }

  @Override
  public synchronized void append(int value) {
    ensureBufferCapacity();
    lastBuffer.putShort((short) value);
    curPos++;
  }

  @Override
  public int get(int index) {
    if (index < startPos) return 0;
    final int pos = index - startPos;
    if (pos >= curPos) return 0;
    final int positionWithinBuffer = pos % BUFFER_SIZE;
    final int bufferIndex = pos / BUFFER_SIZE;
    final int val = this.buffers.get(bufferIndex).getShort(positionWithinBuffer * Short.BYTES);
    // log.debug("column[{}]={} (offset {}, buffer #{})", index, val,
    // positionWithinBuffer, bufferIndex);
    return val;
  }
}
