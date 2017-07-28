package org.cubedb.core.tiny;

import org.cubedb.core.Column;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import gnu.trove.list.TShortList;
import gnu.trove.list.array.TShortArrayList;

public class TinyColumn implements Column, KryoSerializable {
  TShortList data;
  int offset;

  public TinyColumn(int offset) {
    this.init(offset);
  }

  public TinyColumn() {
    this(0);
  }

  protected void init(int offset) {
    this.data = new TShortArrayList(256);
    this.offset = offset;
  }

  @Override
  public void append(int value) {
    data.add((short) value);
  }

  @Override
  public int get(int index) {
    if (index < offset) return 0;
    return data.get(index - offset);
  }

  @Override
  public long size() {
    return data.size() * 2;
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
  public void write(Kryo kryo, Output output) {
    output.writeInt(this.data.size());
    output.writeInt(this.offset);
    for (int i = 0; i < this.data.size(); i++) output.writeShort(this.data.get(i));
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int columnSize = input.readInt();
    int offset = input.readInt();
    init(offset);
    for (int i = 0; i < columnSize; i++) this.data.add(input.readShort());
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
