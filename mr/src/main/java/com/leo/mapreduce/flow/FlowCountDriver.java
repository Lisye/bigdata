package com.leo.mapreduce.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/flowin", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/flowout1"};

        //    1.获取配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//    2.设置jar路径
        job.setJarByClass(FlowCountDriver.class);

//    3. 关联 map reduce
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

//    4.map 输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

//    5.最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//        指定自定义数据分区
//        job.setPartitionerClass(FlowPartitioner.class);
////        同时指定相应数量的 reduce task
//        job.setNumReduceTasks(5);

//    6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//    7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
