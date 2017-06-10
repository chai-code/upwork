package com.upwork.imageencryptor.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.upwork.imageencryptor.mapreduce.util.CompositeKeyWritable;
import com.upwork.imageencryptor.mapreduce.util.ImageBytesWritable;

public class ImageOutputFormat
    extends FileOutputFormat<CompositeKeyWritable, ImageBytesWritable> {

  @Override
  public RecordWriter<CompositeKeyWritable, ImageBytesWritable>
      getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new ImageRecordWriter(context);
  }

}
