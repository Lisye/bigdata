package com.leo.mapreduce.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/orderin/order.txt", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/orderout1"};

        //    conf job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//   jar
        job.setJarByClass(OrderSortDriver.class);

//    mapper reducer
        job.setMapperClass(OrderSortMapper.class);
        job.setReducerClass(OrderSortReducer.class);

//    map out
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

//    end out
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

//     设置reduce端的分组
        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

//    input output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//    submit
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
