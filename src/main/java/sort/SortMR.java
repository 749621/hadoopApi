package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortMR extends Configured implements Tool {

    /**
     * 自定义排序
     * 	排序按照key排序
     *
     * 	自定义key实现排序
     * 		1 实现WritableComparable
     * 		2 实现、RawComparator *
     * 			从字节上比较，效率更高
     */
    public static class SortMapper extends Mapper<LongWritable, Text,MyKey, NullWritable>{

        MyKey keyOut = new MyKey();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            String name = split[0];
            int age = Integer.parseInt(split[1]);
            int salary = Integer.parseInt(split[2]);
            keyOut.setAge(age);
            keyOut.setSalary(salary);
            keyOut.setName(name);
            context.write(keyOut,NullWritable.get());
        }
    }

    public static class SortComparator implements RawComparator<MyKey>{


        /**
         * 
         * @param b1 字节数组
         * @param s1 在b1中的位置
         * @param l1 b1的字节数组长度
         * @param b2 字节数组
         * @param s2 在b2中的位置
         * @param l2 b2字节数组长度
         * @return
         */
        //  [name,salary,age]                               [name,salary,age]
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return   WritableComparator.compareBytes(b1,0,l1,b2,0,l2);

        }

        @Override
        public int compare(MyKey o1, MyKey o2) {
            return 0;
        }
    }

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new SortMR(),args);
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
        job.setMapOutputKeyClass(MyKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapperClass(SortMapper.class);
        job.setCombinerKeyGroupingComparatorClass(SortComparator.class);


        //4、提交任务
        boolean b = job.waitForCompletion(true);
        return b ?1 :0;
    }
}
