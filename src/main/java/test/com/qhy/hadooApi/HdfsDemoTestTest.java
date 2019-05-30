package test.com.qhy.hadooApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.net.URI;

/** 
* HdfsDemoTest Tester. 
* 
* @author <Authors name> 
* @since <pre>四月 24, 2019</pre> 
* @version 1.0 
*/ 
public class HdfsDemoTestTest {

    FileSystem fileSystem = null;

@Before
public void before() throws Exception {

    Configuration configuration = new Configuration();
    System.out.println(configuration+"配置");
    configuration.set("dfs.replication","7");
    fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"),configuration,"hadoop");



} 

@After
public void after() throws Exception {

    boolean delete = fileSystem.delete(new Path("/input"), true);
    System.out.println(delete?"成功":"失败");

} 

/** 
* 
* Method: setUp() 
* 
*/ 
@Test
public void testSetUp() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: deleteTest() 
* 
*/ 
@Test
public void testDeleteTest() throws Exception { 
//TODO: Test goes here... 
} 


} 
