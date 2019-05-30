package mapjoin.reducejoin;

import group.GroupMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class MapJoinMR extends Configured implements Tool {

    //缓存小表数据


    /**
     * mapjoin
     * 适合大小表join
     * 将小表数据缓存于map集合中
     */
    public static class MJMapper extends Mapper<LongWritable, Text,Text,Text>{

        HashMap<String,String> cacheMap = new HashMap<String, String>();
        final String path = "hdfs://hadoop01:8020/data/dept.log";
        Text valueOut = new Text();
        Text keyOut = new Text();
        //task 开始调用一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            //  获取文件系统
            FileSystem fs = null;
            try {
                fs = FileSystem.get(new URI(path),new Configuration(),"hadoop");
                //获取目标文件的字符流
                FSDataInputStream open = fs.open(new Path(path));
                BufferedReader br = new BufferedReader(new InputStreamReader(open));

                String line = null;
                while ((line=br.readLine())!=null){

                    String[] vals = line.split("\t");
                    cacheMap.put(vals[0],vals[1]);
                }
                br.close();
                open.close();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }

        /**
         *
         * @param key in 偏移量
         * @param value emp.log一行
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            //获取字段
            String[] split = value.toString().split("\t");
            //包装key value
            String eid = split[0];
            String did = split[2];
            String ename = split[1];

            String dname = cacheMap.get(did);
            if(dname == null){
                return;
            }
            keyOut.set(eid);
            StringBuffer sb = new StringBuffer();

            sb.append(ename).append("\t").append(did).append("\t").append(dname);

            valueOut.set(sb.toString());
            context.write(keyOut,valueOut);

        }
    }

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new MapJoinMR(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //1、创建Job对象
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        //2、设置输入输出路径
        FileInputFormat.setInputPaths(job,args[0]);
        Path path = new Path(args[1]);
        FileSystem fs = path.getFileSystem(conf);
        //3、如果存在就删除
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,path);
        //3、设置map和reduce对象
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MJMapper.class);


        //4、提交任务
        boolean b = job.waitForCompletion(true);
        return b ?1 :0;
    }


}
