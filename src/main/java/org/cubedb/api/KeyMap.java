package org.cubedb.api;

public interface KeyMap {
Integer get(byte[] b);
void put(byte[] k, int v);
int size();
}
