package com.leo.mapreduce.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NLineDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/NLine.txt", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/NLine"};

//      1  conf job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//      2 jar
        job.setJarByClass(NLineDriver.class);

//      3 map reducer
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

//        4 map out
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

//        5 end out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//       设置如何切分
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        job.setInputFormatClass(NLineInputFormat.class);

//        6 whoin out path
        FileInputFormat.setInputPaths(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        7 submit job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
