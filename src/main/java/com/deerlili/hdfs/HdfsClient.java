package com.deerlili.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author deerlili
 * @date 2022/3/31
 * @apiNote
 */
public class HdfsClient {

    private FileSystem hdfs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop100:9000");
        Configuration configuration = new Configuration();
        //configuration.set("dfs.replication","2");
        String user = "root";
        hdfs = FileSystem.get(uri, configuration,user);
    }

    @After
    public void close() throws IOException {
        hdfs.close();
    }

    @Test
    public void testMkdir() throws IOException {
        hdfs.mkdirs(new Path("/test"));
    }

    /**
     * 参数优先级排序：
     * （1）客户端代码中设置的值
     * （2）ClassPath下的用户自定义配置文件
     * （3）然后是服务器的自定义配置(xxx-site.xml)
     * （4）服务器的默认配置(xxx-default.xml)
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        Path src = new Path("E:\\11-软考高级\\deerlili.txt");
        Path dst = new Path("/test");
        hdfs.copyFromLocalFile(false,true,src,dst);
    }

    @Test
    public void testGet() throws IOException {
        Path src = new Path("/test");
        Path dst = new Path("E:\\");
        hdfs.copyToLocalFile(false,src,dst,true);
    }

    @Test
    public void testRm() throws IOException {
        Path path = new Path("/output");
        hdfs.delete(path,true);
    }

    @Test
    public void testMv() throws IOException {
        Path src = new Path("/input");
        Path dst = new Path("/output");
        hdfs.rename(src,dst);
    }

    @Test
    public void fileDetails() throws IOException {
        Path path = new Path("/");
        RemoteIterator<LocatedFileStatus> listFiles = hdfs.listFiles(path, true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            System.out.println(Arrays.toString(blockLocations));
        }

    }



}
