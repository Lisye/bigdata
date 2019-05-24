package com.leo.mapreduce.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private LongWritable v = new LongWritable(1);
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }

    }
}
