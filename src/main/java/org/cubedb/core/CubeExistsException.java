package org.cubedb.core;

public class CubeExistsException extends Exception {

  public CubeExistsException(String format) {
    super(format);
  }

  private static final long serialVersionUID = -494245440849015730L;
}
