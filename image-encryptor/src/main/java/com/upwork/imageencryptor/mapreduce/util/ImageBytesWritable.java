package com.upwork.imageencryptor.mapreduce.util;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;

public class ImageBytesWritable extends BytesWritable {

  private String type;
  private int chunkIndex;
  private int rowIndex;
  private int colIndex;


  public ImageBytesWritable() {
    super();
  }
  public ImageBytesWritable(BufferedImage image, String type,
      int chunkIndex, int rowIndex, int colIndex) throws IOException {
    setBufferedImage(image, type);
    this.type = type;
    this.chunkIndex = chunkIndex;
    this.rowIndex = rowIndex;
    this.colIndex = colIndex;
  }

  public String getType() {
    return type;
  }

  public int getChunkIndex() {
    return chunkIndex;
  }

  public int getRowIndex() {
    return rowIndex;
  }

  public int getColIndex() {
    return colIndex;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    chunkIndex = WritableUtils.readVInt(in);
    rowIndex = WritableUtils.readVInt(in);
    colIndex = WritableUtils.readVInt(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    WritableUtils.writeVInt(out, chunkIndex);
    WritableUtils.writeVInt(out, rowIndex);
    WritableUtils.writeVInt(out, colIndex);
  }

  public BufferedImage getBufferedImage() throws IOException {
    return ImageIO.read(new ByteArrayInputStream(getBytes()));
  }

  private void setBufferedImage(BufferedImage image, String type) throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    ImageIO.write(image, type, outStream);
    outStream.flush();
    super.set(new BytesWritable(outStream.toByteArray()));
  }

}
