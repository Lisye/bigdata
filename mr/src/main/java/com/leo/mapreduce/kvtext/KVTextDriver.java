package com.leo.mapreduce.kvtext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVTextDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/KV.txt", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/KV"};

        //    1  配置信息
        Configuration conf = new Configuration();
//        设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        Job job = Job.getInstance(conf);

//    2 jar
        job.setJarByClass(KVTextDriver.class);

//    3 map reduce
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

//    4 map out
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

//    5 end out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

//    6 path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//    7 submit
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
