package org.cubedb.offheap;

import org.cubedb.core.Constants;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class MultiBuffer implements KryoSerializable {
  public static final Logger log = LoggerFactory.getLogger(MultiBuffer.class);

  static final int BUFFER_SIZE = Constants.INITIAL_PARTITION_SIZE;
  private int bufferSizeInBytes;
  protected int fieldSize;
  protected int curPos;
  protected int startPos;
  protected List<ByteBuffer> buffers;
  protected ByteBuffer lastBuffer;

  MultiBuffer(int startPos, int fieldSize) {
    init(startPos, fieldSize, 0);
  }

  protected void init(int startPos, int fieldSize, int curPos) {
    this.fieldSize = fieldSize;
    this.startPos = startPos;
    this.bufferSizeInBytes = BUFFER_SIZE * fieldSize;
    this.curPos = curPos;
    buffers = new ArrayList<ByteBuffer>();
  }

  MultiBuffer(int fieldSize) {
    this(0, fieldSize);
  }

  public int getStartPos() {
    return this.startPos;
  }

  protected void ensureBufferCapacity() {
    if (lastBuffer != null && lastBuffer.hasRemaining()) return;
    //log.info("Capacity of buffer not enough, extending it");
    ByteBuffer b = ByteBuffer.allocateDirect(bufferSizeInBytes);
    b.clear();
    buffers.add(b);
    lastBuffer = b;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((buffers == null) ? 0 : buffers.hashCode());
    result = prime * result + ((lastBuffer == null) ? 0 : lastBuffer.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    MultiBuffer other = (MultiBuffer) obj;
    if (buffers == null) {
      if (other.buffers != null) return false;
    } else if (!buffers.equals(other.buffers)) return false;
    if (lastBuffer == null) {
      if (other.lastBuffer != null) return false;
    } else if (!lastBuffer.equals(other.lastBuffer)) return false;
    return true;
  }

  /*
   * public synchronized void append(Consumer<ByteBuffer> op) {
   * ensureBufferCapacity(); lastBuffer.putInt(value); curPos++; }
   *
   *
   *
   * public Object get(int index, BiFunction<>) { if (index < startPos) return
   * null; if (index >= curPos) return null; int pos = index - startPos; int
   * positionWithinBuffer = pos % BUFFER_SIZE; int bufferIndex = pos /
   * BUFFER_SIZE; int val =
   * this.buffers.get(bufferIndex).getInt(positionWithinBuffer *
   * Integer.BYTES); return val; }
   */

  public void free() {
    this.buffers.clear();
    curPos = 0;
  }

  public long size() {
    return this.buffers.size() * (long) bufferSizeInBytes;
  }

  public int getNumBuffers() {
    return this.buffers.size();
  }

  @Override
  public void write(Kryo kryo, Output output) {
    output.writeInt(this.startPos);
    output.writeInt(this.fieldSize);
    int curPos = this.curPos;
    int position = this.lastBuffer.position();
    output.writeInt(curPos);
    output.writeInt(this.buffers.size());
    output.writeInt(position);
    byte[] dst = new byte[this.bufferSizeInBytes];
    for (ByteBuffer buf : this.buffers) {
      int pos = buf.position();
      buf.position(0);
      buf.get(dst);
      buf.position(pos);
      output.write(dst);
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int startPos = input.readInt();
    int fieldSize = input.readInt();
    int curPos = input.readInt();
    init(startPos, fieldSize, curPos);
    int numBuffers = input.readInt();
    int positionWithinLastBuffer = input.readInt();
    byte[] src = new byte[this.bufferSizeInBytes];
    for (int i = 0; i < numBuffers; i++) {
      input.readBytes(src);
      ByteBuffer b = ByteBuffer.allocateDirect(bufferSizeInBytes);
      b.put(src);
      this.buffers.add(b);
      this.lastBuffer = b;
    }
    this.lastBuffer.position(positionWithinLastBuffer);
  }

  public int getNumRecords() {
    return curPos;
  }

  public boolean isTiny() {
    return false;
  }
}
