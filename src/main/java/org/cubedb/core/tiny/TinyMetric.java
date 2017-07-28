package org.cubedb.core.tiny;

import org.cubedb.core.Metric;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import gnu.trove.list.array.TLongArrayList;

public class TinyMetric implements Metric, KryoSerializable {

  TLongArrayList data;
  int offset;

  public TinyMetric(int offset) {
    this.init(offset);
  }

  public TinyMetric() {
    this(0);
  }

  protected void init(int offset) {
    this.data = new TLongArrayList(256);
    this.offset = offset;
  }

  @Override
  public void append(long value) {
    data.add(value);
  }

  @Override
  public long get(int index) {
    if (index < offset) return 0;
    return data.get(index - offset);
  }

  @Override
  public long size() {
    return data.size() * 8;
  }

  @Override
  public void free() {
    this.data.clear();
    this.data = null;
  }

  @Override
  public int getNumBuffers() {
    return 1;
  }

  @Override
  public void incrementBy(int index, long value) {
    int i = index - offset;
    long val = this.data.get(i);
    this.data.setQuick(i, val + value);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    output.writeInt(this.data.size());
    output.writeInt(this.offset);
    for (int i = 0; i < this.data.size(); i++) output.writeLong(this.data.get(i));
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int columnSize = input.readInt();
    int offset = input.readInt();
    init(offset);
    for (int i = 0; i < columnSize; i++) this.data.add(input.readLong());
  }

  @Override
  public int getNumRecords() {
    return data.size();
  }

  @Override
  public boolean isTiny() {
    return true;
  }
}
