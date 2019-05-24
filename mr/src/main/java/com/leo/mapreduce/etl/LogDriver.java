package com.leo.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/etlin", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/etlout"};

//        job conf
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//        jar
        job.setJarByClass(LogDriver.class);

//        mapper reducer
        job.setMapperClass(LogMapper.class);

//        mapper out
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

//        final out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

//        设置reducertask个数
        job.setNumReduceTasks(0);

//        file in/out path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        submit
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
