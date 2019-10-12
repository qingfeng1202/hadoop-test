package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Hadoop HDFS Java API 操作
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://192.168.1.10:9000";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception{
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
    }

    @After
    public void tearDown() throws Exception{
        //释放资源
        configuration = null;
        fileSystem = null;

        System.out.println("HDFSApp.tearDown");
    }

    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/wc/input"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/wc/input/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 查看HDFS文件的内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/wc/input/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }


    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/wc/input/a.txt");
        Path newPath = new Path("/wc/input/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 上传文件到HDFS
     *
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("data/wc");
        Path hdfsPath = new Path("/wc/input");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传文件到HDFS(带进度条)
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("D:\\hadoop\\hadoop-2.6.0-cdh5.16.1.tar.gz")));

        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/hadoop-2.6.0-cdh5.16.1.tar.gz"),
                new Progressable() {
                    public void progress() {
                        System.out.print(".");  //带进度提醒信息
                    }
                });


        IOUtils.copyBytes(in, output, 4096);
    }


    /**
     * 下载HDFS文件
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("C:\\Users\\qingfeng\\Desktop\\b.doc");
        Path hdfsPath = new Path("/hdfsapi/test/a.doc");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath,true);
    }

    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));

        for(FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            //副本个数，如果是通过hdfs shell的方式put的上去的那么，才采用默认的副本系数1
            // 如果我们是java api上传上去的，在本地我们并没有手工设置副本系数，所以否则采用的是hadoop自己的副本系数
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }

    }

    /**
     * 删除
     */
    @Test
    public void delete() throws Exception{
        //true,表示递归删除
        fileSystem.delete(new Path("/usr"), true);
    }


}
