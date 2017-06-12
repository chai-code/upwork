package com.upwork.imageencryptor.mapreduce.input;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.upwork.imageencryptor.mapreduce.util.CompositeKeyWritable;
import com.upwork.imageencryptor.mapreduce.util.ImageBytesWritable;

public class ImageFileRecordReader
    extends RecordReader<CompositeKeyWritable, ImageBytesWritable> {

  private FileSplit split;
  private FileSystem fs;
  private InputStream in;
  private int totalRecords;
  private int totalRows;
  private int totalCols;
  private int currentRecordCount;
  private int currentRow;
  private int currentCol;
  private int recordWidth;
  private int recordHeight;
  private int imgType;
  private BufferedImage image;
  private String fileName;
  private CompositeKeyWritable currentKey;
  private ImageBytesWritable currentValue;
  private String type;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.split = (FileSplit)split;
    this.fileName = this.split.getPath().getName();
    this.type = this.fileName.substring(fileName.lastIndexOf(".")+1);
    this.fs = FileSystem.get(context.getConfiguration());
    this.in = fs.open(this.split.getPath());
    this.image = ImageIO.read(this.in);
    this.imgType = image.getType();
    this.totalRows = (int) Math.ceil(image.getWidth() / 1000.0);
    this.totalCols = (int) Math.ceil(image.getHeight() / 1000.0);
    this.recordWidth = image.getWidth() / totalCols;
    this.recordHeight = image.getHeight() / totalRows;
    this.totalRecords = totalRows * totalCols;
    this.currentRecordCount = 0;
    this.currentRow = 0;
    this.currentCol = 0;
  }

  @Override
  public CompositeKeyWritable getCurrentKey() throws IOException,
      InterruptedException {
    if(currentKey == null) {
      nextKeyValue();
    }
    return currentKey;
  }

  @Override
  public ImageBytesWritable getCurrentValue() throws IOException,
      InterruptedException {
    if(currentValue == null) {
      nextKeyValue();
    }
    return currentValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float)totalRecords / (float)currentRecordCount;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if(currentRecordCount < totalRecords ) {
      BufferedImage imageRecord = new BufferedImage(recordWidth, recordHeight,
          imgType);
      Graphics2D graphics = imageRecord.createGraphics();
      graphics.drawImage(image, 0, 0,
          recordWidth, recordHeight,
          recordWidth * currentCol, recordHeight * currentRow,
          recordWidth * currentCol + recordWidth,
          recordHeight * currentRow + recordHeight, null);
      graphics.dispose();
      imageRecord.flush();
      currentKey = new CompositeKeyWritable(fileName, currentRecordCount);
      currentValue = new ImageBytesWritable(imageRecord, type, currentRecordCount,
          currentRow, currentCol);
      if(currentRow < totalRows) {
        if(currentCol < totalCols - 1) {
          currentCol++;
        } else {
          currentCol = 0;
          currentRow++;
        }
      }
      currentRecordCount++;
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    in.close();
    fs.close();
  }

}
