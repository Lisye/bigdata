package com.leo.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilerRecoderWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;

    public FilerRecoderWriter(TaskAttemptContext job) {
//        1  获取文件系统
        FileSystem fs;

        try {
//        2 创建文件输出路径
            fs = FileSystem.get(job.getConfiguration());
//        3 创建输出流
            atguiguOut = fs.create(new Path("/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/outputformatout/auguigu.log"));
            otherOut = fs.create(new Path("/Users/leo/work/IdeaProject/hadoop/mr/src/main/resources/outputformatout/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("atguigu")) {
            atguiguOut.write(key.getBytes());
        } else {
            otherOut.write(key.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
