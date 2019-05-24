package com.leo.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    private Configuration conf;
    private FileSplit split;

    private Text k = new Text();
    private BytesWritable v = new BytesWritable();

    Boolean isProgress = true;

    @Override

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        this.split = (FileSplit) split;
        conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProgress) {
            byte[] contents = new byte[(int) split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                Path path = split.getPath();
                fs = path.getFileSystem(conf);
                fis = fs.open(path);

                IOUtils.readFully(fis, contents, 0, contents.length);
                v.set(contents, 0, contents.length);

                String name = split.getPath().getName();
                k.set(name);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
            }

            isProgress = false;

            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
