package HDFS_Demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        //获取HDFS文件对象
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "atguigu");
        System.out.println("Before Method：Creating HDFS object……");
    }

    @Test
    public void put() throws IOException, InterruptedException {
        //设置配置文件
        Configuration configuration = new Configuration();
        //自定义配置
        //configuration.setInt("dfs.replication",1);
        //为HDFS对象传参数：URI、configuration,"username"
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), configuration, "atguigu");
        //利用HDFS文件对象，上传文件
        fs.copyFromLocalFile(new Path("d:/1.txt"), new Path("/3.txt"));
    }

    @Test
    public void get() throws IOException, InterruptedException {

        //获取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration, "atguigu");

        //利用HDFS文件对象，下载文件
        fileSystem.copyToLocalFile(new Path("/test"), new Path("d:\\"));
        //利用HDFS文件对象，下载文件
        fs.copyToLocalFile(new Path("/3.txt"), new Path("d:\\"));
        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void rename() throws IOException, InterruptedException {
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "atguigu");

        //操作
        fileSystem.rename(new Path("/test"), new Path("/test2"));

        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void delete() throws IOException {
        boolean delete = fs.delete(new Path("/1.txt"), true);
        if (delete) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }

    @Test
    public void du() throws IOException {
        //
        FSDataOutputStream append = fs.append(new Path("/test2/1.txt"), 1024);
        //创建本地输入流
        FileInputStream open = new FileInputStream("d:\\1.txt");
        //流拷贝
        IOUtils.copyBytes(open, append, 1024, true);
    }

    @Test
    public void ls() throws IOException {
        //查看文件状态
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("一下信息是一个文件的信息");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
            } else {
                System.out.println("这是一个文件夹");
                System.out.println(fileStatus.getPath());
            }
        }

    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            System.out.println("======================================");
            System.out.println(file.getPath());

            System.out.println("块信息：");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.print("块在");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
            }

        }
    }

    @After
    public void after() throws IOException {
        //关闭HDFS对象，防止别人写不了
        System.out.println("After Method is running……");
        fs.close();
    }


}
