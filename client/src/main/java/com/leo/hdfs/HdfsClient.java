package com.leo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    /**
     * HDFS文件上传（测试参数优先级）:
     * 参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        fs.copyFromLocalFile(new Path("/Users/leo/zhangfei.txt"), new Path("/sanguo/shuguo"));
        fs.close();

        System.out.println("over");
    }

    /**
     * 删除文件 HDFS文件夹删除
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        fs.delete(new Path("/sanguo/shuguo/zhangfei.txt"), true);
        fs.close();

        System.out.println("over");
    }

    /**
     * HDFS文件下载
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        fs.copyToLocalFile(new Path("/sanguo/shuguo/guanyu.txt"), new Path("/Users/leo/work/IdeaProject/hadoopclient/src/main/resources/"));

        fs.close();

        System.out.println("over");
    }

    /**
     *HDFS文件名更改
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        fs.rename(new Path("/sanguo/shuguo/zhangfei.txt"), new Path("/sanguo/shuguo/feifei.txt"));
        fs.close();

        System.out.println("over");
    }

    /**
     * HDFS文件详情查看
     * 查看文件名称、权限、长度、块信息
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            System.out.println(status.getPath().getName());
            System.out.println(status.getLen());
            System.out.println(status.getPermission());
            System.out.println(status.getGroup());

            BlockLocation[] blocks = status.getBlockLocations();

            for (BlockLocation block : blocks) {
                String[] hosts = block.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }

            }

            System.out.println("---------------------------");
        }

        fs.close();

        System.out.println("over");
    }

    /**
     * HDFS文件和文件夹判断
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        FileStatus[] statuses = fs.listStatus(new Path("/"));

        for (FileStatus status : statuses) {
            if (status.isFile()) {
                System.out.println("file: " + status.getPath().getName());
            } else {
                System.out.println("dir: " + status.getPath().getName());
            }
        }

    }

    /**
     * 把本地文件上传到HDFS目录
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void putFileToHdfs() throws URISyntaxException, IOException, InterruptedException {
//        获取链接
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

//        输入流
        FileInputStream fis = new FileInputStream(new File("/Users/leo/work/IdeaProject/hadoopclient/src/main/resources/banhua.txt"));

//        输出流
        FSDataOutputStream fos = fs.create(new Path("/sanguo/shuguo/banhua.txt"));

//        对等拷贝
        IOUtils.copyBytes(fis, fos, conf);

//        关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();
    }

    /**
     * 从HDFS上下载banhua.txt文件到本地e盘上
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void getFileFromHdfs() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        FSDataInputStream fis = fs.open(new Path("/sanguo/shuguo/banhua.txt"));

        FileOutputStream fos = new FileOutputStream(new File("/Users/leo/work/IdeaProject/hadoopclient/src/main/resources/xiaohua.txt"));

        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();

        System.out.println("over");
    }

    /**
     * 需求：分块读取HDFS上的大文件，比如根目录下的/hadoop-2.7.2.tar.gz
     * 下载第一块
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        FileOutputStream fos = new FileOutputStream(new File("/Users/leo/work/IdeaProject/hadoopclient/src/main/resources/hadoop-2.7.2.tar.gz.part1"));

        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();

        System.out.println("over");
    }

    /**
     * 下载第一块
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "leo");

        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        fis.seek(1024*1024*128);

        FileOutputStream fos = new FileOutputStream(new File("/Users/leo/work/IdeaProject/hadoopclient/src/main/resources/hadoop-2.7.2.tar.gz.part2"));

        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();

        System.out.println("over");
    }
}
