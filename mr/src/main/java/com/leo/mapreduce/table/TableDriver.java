package com.leo.mapreduce.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/tablein", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/tableout"};

//        conf job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//        jar
        job.setJarByClass(TableDriver.class);

//        mapper reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

//        map out
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

//        end out
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

//        in out path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        submit
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
