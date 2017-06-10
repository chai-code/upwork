package com.upwork.imageencryptor.mapreduce.util;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.BytesWritable;

public class ImageBytesWritable extends BytesWritable {

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
  }

  public BufferedImage getBufferedImage() throws IOException {
    return ImageIO.read(new ByteArrayInputStream(getBytes()));
  }

  public void setBufferedImage(BufferedImage image) throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    ImageIO.write(image, "bmp", outStream);
    super.set(new BytesWritable(outStream.toByteArray()));
  }

}
