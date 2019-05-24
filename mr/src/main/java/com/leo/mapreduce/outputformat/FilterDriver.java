package com.leo.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/outputformatin", "/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/outputformatout"};

//        conf job
        Configuration conf = new Configuration();
        Job job = Job.getInstance();

//        jar
        job.setJarByClass(FilterDriver.class);

//        mapper reducer
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

//        map out
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

//        end out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

//        要将自定义的输出格式组件设置到job中
        job.setOutputFormatClass(FilterOutputFormat.class);

//        in out path
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


//        submit
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
