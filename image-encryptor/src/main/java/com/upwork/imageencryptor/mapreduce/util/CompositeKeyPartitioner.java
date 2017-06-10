package com.upwork.imageencryptor.mapreduce.util;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class CompositeKeyPartitioner
    extends Partitioner<CompositeKeyWritable, NullWritable>{

  @Override
  public int getPartition(CompositeKeyWritable key, NullWritable value,
      int numReduceTasks) {
    return (key.getFileName().hashCode() % numReduceTasks);
  }

}
