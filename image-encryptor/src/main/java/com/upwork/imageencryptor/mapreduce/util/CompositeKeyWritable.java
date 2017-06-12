package com.upwork.imageencryptor.mapreduce.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,
    WritableComparable<CompositeKeyWritable> {

  private String fileName;
  private int chunkIndex;

  public CompositeKeyWritable() {
    super();
  }
  public CompositeKeyWritable(String fileName,
      int chunkIndex) {
    this.fileName = fileName;
    this.chunkIndex = chunkIndex;
  }

  public String getFileName() {
    return fileName;
  }

  public int getChunkIndex() {
    return chunkIndex;
  }

  @Override
  public void readFields(DataInput dataInput)
      throws IOException {
    fileName = WritableUtils.readString(dataInput);
    chunkIndex = WritableUtils.readVInt(dataInput);
  }

  @Override
  public void write(DataOutput dataOut) throws IOException {
    WritableUtils.writeString(dataOut, fileName);
    WritableUtils.writeVInt(dataOut, chunkIndex);
  }

  @Override
  public int compareTo(CompositeKeyWritable object) {
    int result = fileName.compareTo(object.fileName);
    return result == 0 ? Integer.compare(chunkIndex, object.chunkIndex) :
        result;
  }

  @Override
  public String toString() {
    return "Name: " + fileName +
        " Index: " + chunkIndex;
  }

}
