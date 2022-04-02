package com.deerlili.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(HdfsClient.class);

    private FileSystem hdfs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop100:9000");
        Configuration configuration = new Configuration();
        //configuration.set("dfs.replication","2");
        String user = "root";
        hdfs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        hdfs.close();
    }

    @Test
    public void testMkdir() throws IOException {
        Path path = new Path("/test");
        hdfs.mkdirs(path);
    }

    /**
     * 参数优先级排序：
     * （1）客户端代码中设置的值
     * （2）ClassPath下的用户自定义配置文件
     * （3）然后是服务器的自定义配置(xxx-site.xml)
     * （4）服务器的默认配置(xxx-default.xml)
     *
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        Path src = new Path("E:\\11-软考高级\\deerlili.txt");
        Path dst = new Path("/test");
        hdfs.copyFromLocalFile(false, true, src, dst);
    }

    @Test
    public void testGet() throws IOException {
        Path src = new Path("/test");
        Path dst = new Path("E:\\");
        hdfs.copyToLocalFile(false, src, dst, true);
    }

    @Test
    public void testRm() throws IOException {
        Path path = new Path("/output");
        hdfs.delete(path, true);
    }

    @Test
    public void testMv() throws IOException {
        Path src = new Path("/input");
        Path dst = new Path("/output");
        hdfs.rename(src, dst);
    }

    @Test
    public void fileDetails() throws IOException {
        Path path = new Path("/");
        RemoteIterator<LocatedFileStatus> listFiles = hdfs.listFiles(path, true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            logger.info("file info: {}", fileStatus.getPath().getName());
            logger.info("file info: {}", fileStatus.getPermission());
            logger.info("file info: {}", fileStatus.getOwner());
            logger.info("file info: {}", fileStatus.getGroup());
            logger.info("file info: {}", fileStatus.getLen());
            logger.info("file info: {}", fileStatus.getModificationTime());
            logger.info("file info: {}", fileStatus.getReplication());
            logger.info("file info: {}", fileStatus.getBlockSize());


            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            logger.info("file info: {}", Arrays.toString(blockLocations));
        }
    }

    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = hdfs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                logger.info("文件: {}", status.getPath().getName());
            } else {
                logger.info("目录: {}", status.getPath().getName());
            }
        }
    }
}
