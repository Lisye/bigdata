package com.leo.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 目标:将 fruit 表中的一部分数据，通过 MR 迁入到 fruit_mr 表中。
 *
 * 3.构建 Fruit2FruitDriver extends Configured implements Tool 用于组装运行 Job 任务
 */
public class Fruit2FruitDriver extends Configuration implements Tool {
    private Configuration configuration = null;

    public int run(String[] strings) throws Exception {

        //得到 Configuration, 创建 Job 任务
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());

        // Driver
        job.setJarByClass(Fruit2FruitDriver.class);

        //设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者 是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit",                      //数据源的表名
                new Scan(),                               //scan扫描控制器
                ReadFruitMapper.class,              //设置 Mapper 类
                ImmutableBytesWritable.class,       //设置 Mapper 输出 key 类型
                Put.class,                          //设置 Mapper 输出 value 值类型
                job                                 //设置给哪个 JOB
        );

        //设置 Reducer
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr",
                WriteFruitMRReducer.class,
                job
        );
        job.setNumReduceTasks(1); //设置 Reduce 数量，最少 1 个

        //提交
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        int b = ToolRunner.run(conf, new Fruit2FruitDriver(), args);
    }
}
