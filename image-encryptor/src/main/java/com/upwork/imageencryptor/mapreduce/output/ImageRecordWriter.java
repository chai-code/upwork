package com.upwork.imageencryptor.mapreduce.output;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

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
  private Map<String, List<BufferedImage>> outMap = new HashMap<>();
  private int totalWidth;
  private int totalHeight;
  private int rows;
  private int cols;

  public ImageRecordWriter(TaskAttemptContext context) throws IOException {
    this.conf = context.getConfiguration();
    this.fs = FileSystem.get(conf);
  }

  @Override
  public void write(CompositeKeyWritable key, ImageBytesWritable value)
      throws IOException, InterruptedException {
    String fileName = key.getFileName();
    if(!outMap.containsKey(fileName)) {
      outMap.put(fileName, new ArrayList<BufferedImage>());
    }
    BufferedImage image = value.getBufferedImage();
    totalWidth = totalWidth + image.getWidth();
    totalHeight = totalHeight + image.getHeight();
    rows = value.getRowIndex();
    cols = value.getColIndex();
    outMap.get(fileName).add(image);

    //part image is printed here for testing
    Path path = new Path(conf.get(FileOutputFormat.OUTDIR), fileName + "_part_" + value.getChunkIndex());
    OutputStream stream = fs.create(path);
    String type = fileName.substring(fileName.lastIndexOf(".")+1);
    ImageIO.write(value.getBufferedImage(), type, stream);
    stream.close();
  }

  public void flush() throws IOException,
      InterruptedException {
    for(Map.Entry<String, List<BufferedImage>> entry : outMap.entrySet()) {
      String fileName = entry.getKey();
      List<BufferedImage> images = entry.getValue();
      //TODO: join the split image files over here from BufferedImage
    }
  }

  @Override
  public void close(TaskAttemptContext context){
    //Do nothing
  }

}
