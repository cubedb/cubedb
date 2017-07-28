package org.cubedb.core;

public class ColumnDoesNotExistException extends Exception {

  private static final long serialVersionUID = -882805029308623644L;

  public ColumnDoesNotExistException(String msg) {
    super(msg);
  }
}
