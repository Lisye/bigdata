package com.leo.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/sortin", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/sortout"};

        //    conf job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//    jar
        job.setJarByClass(FlowCountSortDriver.class);

//    mapper reducer
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

//    mapper out
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

//   end  out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//        加载自定义分区类
        job.setPartitionerClass(FlowCountPartitioner.class);
//        设置Reducetask 个数
        job.setNumReduceTasks(5);

//    input output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//    submit
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
