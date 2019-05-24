package com.leo.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Txt2FruitDriver extends Configuration implements Tool {
    private Configuration configuration = null;

    public int run(String[] args) throws Exception {
//        conf job
        Configuration conf = new Configuration();
        Job job = Job.getInstance();

//        driver
        job.setJarByClass(Txt2FruitDriver.class);

//        mapper
        job.setMapperClass(ReadHDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

//        reducer
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr2",
                Write2HbaseReducer.class,
                job
        );

//        set input path
        FileInputFormat.setInputPaths(job, args[0]);

//        submit
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new Txt2FruitDriver(), args);
    }
}
