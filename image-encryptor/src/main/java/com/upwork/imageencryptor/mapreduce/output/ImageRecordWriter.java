package com.upwork.imageencryptor.mapreduce.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.upwork.imageencryptor.mapreduce.util.CompositeKeyWritable;
import com.upwork.imageencryptor.mapreduce.util.ImageBytesWritable;

public class ImageRecordWriter
    extends RecordWriter<CompositeKeyWritable, ImageBytesWritable>{

  private Configuration conf;
  private FileSystem fs;
  private Map<String, OutputStream> outMap;

  public ImageRecordWriter(TaskAttemptContext context) throws IOException {
    this.conf = context.getConfiguration();
    this.fs = FileSystem.get(conf);
    this.outMap = new HashMap<>();
  }

  @Override
  public void write(CompositeKeyWritable key, ImageBytesWritable value)
      throws IOException, InterruptedException {
    String fileName = key.getFileName();
    if(!outMap.containsKey(fileName)) {
      Path path = new Path(conf.get(FileOutputFormat.OUTDIR), fileName);
      outMap.put(fileName, fs.create(path));
    }
    OutputStream out = outMap.get(fileName);
    byte[] imageContent = value.getBytes();
    if(imageContent != null) {
      out.write(imageContent);
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException,
      InterruptedException {
    for(OutputStream stream : outMap.values()) {
      stream.close();
    }
  }

}
