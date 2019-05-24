package com.leo.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadHDFSMapper extends Mapper<LongWritable, Text, NullWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        String rowKey = fields[0];
        String name = fields[1];
        String color = fields[2];


        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(color));

        context.write(NullWritable.get(), put);
    }
}
