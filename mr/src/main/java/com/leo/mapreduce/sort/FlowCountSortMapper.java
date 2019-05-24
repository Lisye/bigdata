package com.leo.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean k = new FlowBean();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        k.set(upFlow, downFlow);
        String phNum = fields[0];
        v.set(phNum);

        context.write(k, v);
    }
}
