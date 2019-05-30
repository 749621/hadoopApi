package mapjoin.reducejoin;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinMR extends Configured implements Tool {

    /**
     * map   将数据打标记
     *      * 以关联字段作为keyOut
     *      keyin:偏移量
     *      valuein：一行文本
     *              emp ： eid ename did
     *              dept：did dname
     *      keyout：关联字段
     *      valueout：剩下的字段 dname| （eid，ename）
     *
     *      dname d
     *      eid ename e
     */
    public static class RJoinMR extends Mapper<LongWritable, Text,Text,Text>{

        Text keyOut = new Text();
        Text valueOut = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取数据所属文件的路径
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String path = inputSplit.getPath().getName();
            //判断并打标记
            String[] vals = value.toString().split("\t");
            if(path.contains("emp")){

                keyOut.set(vals[2]);
                valueOut.set(vals[0]+"\t"+vals[1]+"\te");
            }else{

                keyOut.set(vals[0]);
                valueOut.set(vals[1]+"\td");
            }

            context.write(keyOut,valueOut);
        }
    }

    /**
     *
     * 分组的数据：
     * 《1，【（1001 jack e），（技术部 d），（1002 rose e）】》
     * reduce 将相同key的数据区分出来自哪些文件并进行拼接。
     */
    public static class RJionReducer extends Reducer <Text,Text,Text,Text>{

        List<String> left = new ArrayList<String>();
        Text valueOut = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            left.clear();
            //分别取出员工和部门
            String dname = null;
            for (Text tmp : values){

                String[] vals = tmp.toString().split("\t");
                String mark = vals[vals.length - 1];
                if("e".equals(mark)){
                    left.add(vals[0]+"\t"+vals[1]);
                }else{
                    dname = (vals[0]);
                }
            }
            for (String emp :left){
                valueOut.set(emp + "\t" + dname);
                context.write(key,valueOut);
            }

        }
    }



    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new ReduceJoinMR(),args);
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
        job.setMapperClass(RJoinMR.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(RJionReducer.class);

        //4、提交任务
        boolean b = job.waitForCompletion(true);
        return b ?1 :0;
    }
}
