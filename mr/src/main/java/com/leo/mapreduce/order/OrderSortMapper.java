package com.leo.mapreduce.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderSortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        int order_id = Integer.parseInt(fields[0]);
        double price = Double.parseDouble(fields[2]);

        k.setOrder_id(order_id);
        k.setPrice(price);

        context.write(k, NullWritable.get());
    }
}
