package com.upwork.imageencryptor.mapreduce.util;

import org.apache.hadoop.mapreduce.Partitioner;


public class CompositeKeyPartitioner
    extends Partitioner<CompositeKeyWritable, ImageBytesWritable>{

  @Override
  public int getPartition(CompositeKeyWritable key, ImageBytesWritable value,
      int numReduceTasks) {
    return (key.getFileName().hashCode() % numReduceTasks);
  }

}
