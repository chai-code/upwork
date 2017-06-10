package com.upwork.imageencryptor.mapreduce.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;;


public class CompositeKeySortComparator
    extends WritableComparator{

  public CompositeKeySortComparator() {
    super(CompositeKeyWritable.class, true);
  }

  @Override
  public int compare(WritableComparable one, WritableComparable two) {
    CompositeKeyWritable keyOne = (CompositeKeyWritable) one;
    CompositeKeyWritable keyTwo = (CompositeKeyWritable) two;
    int result = keyOne.getFileName().compareTo(keyTwo.getFileName());
    return result == 0 ? Integer.compare(keyOne.getChunkIndex(),
        keyTwo.getChunkIndex()) : result;
  }
}
