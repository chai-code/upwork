package com.upwork.imageencryptor.mapreduce;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import com.upwork.imageencryptor.mapreduce.util.CompositeKeyWritable;
import com.upwork.imageencryptor.mapreduce.util.ImageBytesWritable;

public class ImageEncryptionMapper extends
    Mapper<CompositeKeyWritable, ImageBytesWritable,
    CompositeKeyWritable, ImageBytesWritable> {

  @Override
  public void map(CompositeKeyWritable key, ImageBytesWritable value,
      Context context) throws IOException, InterruptedException {
    BufferedImage image = value.getBufferedImage();
    int encryptKey = 10;
    int width = image.getWidth();
    int height = image.getHeight();
    for(int i = 0; i < width; i++) {
      for(int j = 0; j < height; j++){
        Color color = new Color(image.getRGB(i,j));
        Color newColor = new Color(color.getRed() ^ encryptKey,
            color.getGreen() ^ encryptKey,
            color.getBlue() ^ encryptKey);
        image.setRGB(i, j, newColor.getRed());
      }
    }
    ImageBytesWritable encyptedImage = new ImageBytesWritable(image, value.getType(),
        value.getChunkIndex(), value.getRowIndex(), value.getColIndex());
   context.write(key, value);
  }

}
