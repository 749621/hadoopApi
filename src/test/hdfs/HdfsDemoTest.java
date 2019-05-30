import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class HdfsDemoTest {

    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {


        Configuration configuration = new Configuration();
        System.out.println(configuration+"配置");
        configuration.set("dfs.replication","7");
        fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"),configuration,"hadoop");

    }

    //删除
    @Test
    public void deleteTest() throws IOException {

        boolean delete = fileSystem.delete(new Path("/result"), true);
        System.out.println(delete?"成功":"失败");
    }

    //创建
    @Test
    public void mkdirTest() throws IOException {

        boolean mkdir = fileSystem.mkdirs(new Path("/input"));
        System.out.println(mkdir);
    }

    //上传
    @Test
    public void uploadTest() throws IOException {

        //获取被上传文件的输入流
        FileInputStream fileInputStream = new FileInputStream("/Users/qhy/test.txt");
        //获取指向文件上传的输出流
        FSDataOutputStream upload = fileSystem.create(new Path("/result/test.log"));
        //IO工具，执行读写操作  设置缓冲区大小
        IOUtils.copyBytes(fileInputStream,upload,1024*1024);
    }


    //下载
    @Test
    public void downTest() throws IOException {

        FSDataInputStream down = fileSystem.open(new Path("/result/test.log"));
        FileOutputStream fileOutputStream = new FileOutputStream("/Users/qhy/test.log");
        IOUtils.copyBytes(down,fileOutputStream,1024*4);
    }

    //查看（）列出快的列表
    @Test
    public void list() throws IOException {

        RemoteIterator<LocatedFileStatus> list = fileSystem.listFiles(new Path("/result/test.log"),true);
        while (list.hasNext()){

            LocatedFileStatus next = list.next();
            System.out.println("块的大小"+next.getBlockSize());
            System.out.println("len："+next.getLen());
            System.out.println("own："+next.getOwner());
            System.out.println("rep："+next.getReplication());
            System.out.println("perm："+next.getPermission());
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation tmp : blockLocations){
                System.out.println(tmp.getLength());
                System.out.println(tmp.getOffset());
                System.out.println(Arrays.toString(tmp.getHosts()));
                System.out.println(tmp.getNames());
            }
        }

    }


}

