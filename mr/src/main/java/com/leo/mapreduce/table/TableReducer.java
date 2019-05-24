package com.leo.mapreduce.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

//        1 将数据加载到内存中
        for (TableBean value : values) {

            TableBean orderBean = new TableBean();

            if ("order".equals(value.getFlag())) {
                try {
                    BeanUtils.copyProperties(orderBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

        }

        //            2 表的拼接
        for (TableBean order : orderBeans) {
            order.setPname(pdBean.getPname());

            context.write(order, NullWritable.get());
        }
    }
}
