package com.leo.hbase.mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 1.用于读取 fruit 表中的数据
 */
public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());

        Cell[] cells = value.rawCells();
        for (Cell cell : cells) {
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {

                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                if ("name".equals(qualifier)) {
                    put.add(cell);
                } else if ("color".equals(qualifier)) {
                    put.add(cell);
                }
            }
        }

        //将从 fruit 读取到的每行数据写入到 context 中作为 map 的输出 context.write(key, put);
        context.write(key, put);
    }
}
