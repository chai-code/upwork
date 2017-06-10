package com.upwork.imageencryptor.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.upwork.imageencryptor.mapreduce.util.CompositeKeyWritable;
import com.upwork.imageencryptor.mapreduce.util.ImageBytesWritable;

public class ImageInputFormat
    extends FileInputFormat<CompositeKeyWritable, ImageBytesWritable> {

  @Override
  public RecordReader<CompositeKeyWritable, ImageBytesWritable>
      createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new ImageFileRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<>();
    List<FileStatus> files = listStatus(job);
    for(FileStatus file : files) {
      Path path = file.getPath();
      long length = file.getLen();
      if(length != 0) {
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
      } else {
        splits.add(new FileSplit(path, 0, length, new String[0]));
      }
    }
    return splits;
  }

}
