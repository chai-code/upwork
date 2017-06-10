package com.upwork.imageencryptor.mapreduce.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class GroupingComparator
    extends WritableComparator {

  public GroupingComparator() {
    super(CompositeKeyWritable.class, true);
  }

  @Override
  public int compare(WritableComparable one, WritableComparable two) {
    CompositeKeyWritable keyOne = (CompositeKeyWritable) one;
    CompositeKeyWritable keyTwo = (CompositeKeyWritable) two;
    return keyOne.getFileName().compareTo(keyTwo.getFileName());
  }

}
