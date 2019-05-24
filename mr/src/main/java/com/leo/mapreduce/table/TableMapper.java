package com.leo.mapreduce.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private String name;
    private TableBean v = new TableBean();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        if (name.startsWith("order")) {
            v.setOrder_id(fields[0]);
            v.setP_id(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            v.setFlag("order");

            k.set(fields[1]);
        } else {

            v.setP_id(fields[0]);
            v.setPname(fields[1]);
            v.setAmount(0);
            v.setOrder_id("");
            v.setFlag("pd");
            k.set(fields[0]);
        }

        context.write(k, v);
    }
}
