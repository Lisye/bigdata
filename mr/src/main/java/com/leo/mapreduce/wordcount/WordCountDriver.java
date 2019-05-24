package com.leo.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/combinerin", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/combinerout"};

//        1.获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//        2.设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

//        3.设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

//        4.设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        5.设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);

//虚拟存储切片最大值设置20m
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

//        方案一： 在驱动类中指定自定义的Combiner
//        job.setCombinerClass(WordcountCombiner.class);

//        方案二：指定需要使用Combiner，以及用哪个类作为Combiner的逻辑
        job.setCombinerClass(WordCountReducer.class);


//        6.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        7.提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
