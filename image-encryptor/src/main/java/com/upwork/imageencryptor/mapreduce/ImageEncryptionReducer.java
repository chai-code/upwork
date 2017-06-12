package com.upwork.imageencryptor.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.upwork.imageencryptor.mapreduce.output.ImageRecordWriter;
import com.upwork.imageencryptor.mapreduce.util.CompositeKeyWritable;
import com.upwork.imageencryptor.mapreduce.util.ImageBytesWritable;

public class ImageEncryptionReducer
    extends Reducer<CompositeKeyWritable, ImageBytesWritable,
    CompositeKeyWritable, ImageBytesWritable> {

  private ImageRecordWriter writer;

  @Override
  public void setup(Context context) throws IOException {
    writer = new ImageRecordWriter(context);
  }

  @Override
  public void reduce(CompositeKeyWritable key,
      Iterable<ImageBytesWritable> values,
      Context context) throws IOException, InterruptedException {
    for(ImageBytesWritable image : values) {
      writer.write(key, image);
    }
  }

  @Override
  public void cleanup(Context context) throws IOException,
  InterruptedException {
    writer.flush();
  }

}
