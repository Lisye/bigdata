package com.leo.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long upSum = 0;
        long downSum = 0;
        for (FlowBean flowBean : values) {
            upSum += flowBean.getDownFlow();
            downSum += flowBean.getUpFlow();
        }

        FlowBean v = new FlowBean(upSum, downSum);

        context.write(key, v);
    }
}
