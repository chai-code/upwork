package com.upwork.imageencryptor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.upwork.imageencryptor.mapreduce.ImageEncryptionMapper;
import com.upwork.imageencryptor.mapreduce.ImageEncryptionReducer;
import com.upwork.imageencryptor.mapreduce.input.ImageInputFormat;
import com.upwork.imageencryptor.mapreduce.output.ImageOutputFormat;
import com.upwork.imageencryptor.mapreduce.util.CompositeKeyPartitioner;
import com.upwork.imageencryptor.mapreduce.util.CompositeKeySortComparator;
import com.upwork.imageencryptor.mapreduce.util.CompositeKeyWritable;
import com.upwork.imageencryptor.mapreduce.util.GroupingComparator;
import com.upwork.imageencryptor.mapreduce.util.ImageBytesWritable;

public class ImageEncryptor extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if(args.length < 3) {
      System.out.println("Missing Arguments.");
      printUsage();
      return -1;
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    int numberOfReducers = Integer.parseInt(args[2]);
    Job job = Job.getInstance(getConf());
    job.setJobName("image-encryptor");
    job.setJarByClass(ImageEncryptor.class);
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setInputFormatClass(ImageInputFormat.class);
    job.setOutputFormatClass(ImageOutputFormat.class);
    job.setMapperClass(ImageEncryptionMapper.class);
    job.setMapOutputKeyClass(CompositeKeyWritable.class);
    job.setMapOutputValueClass(ImageBytesWritable.class);
    job.setPartitionerClass(CompositeKeyPartitioner.class);
    job.setSortComparatorClass(CompositeKeySortComparator.class);
    job.setGroupingComparatorClass(GroupingComparator.class);
    job.setReducerClass(ImageEncryptionReducer.class);
    job.setNumReduceTasks(numberOfReducers);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  private void printUsage() {
    System.out.println("Usage: ");
    System.out.println(this.getClass().getCanonicalName() +
        "<input dir> <output dir> <number of reducers>");
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int exitCode = ToolRunner.run(conf, new ImageEncryptor(), args);
    System.exit(exitCode);
  }
}
