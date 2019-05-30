package group;

import flowkeyvalue.Flow;
import flowkeyvalue.FlowMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 按照薪资 、name排序
 */
public class GroupMR extends Configured implements Tool {

    public static class GroupMapper extends Mapper<LongWritable, Text,Mykey, DoubleWritable>{


        Mykey keyOut =new Mykey();
        DoubleWritable valueOut = new DoubleWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] valus = value.toString().split("\t");
            double d = Double.parseDouble(valus[1]);
            keyOut.setName(valus[0]);
            keyOut.setSalary(Double.parseDouble(valus[1]));
            valueOut.set(d);
            System.out.println("map输出："+keyOut+":"+valueOut);
            context.write(keyOut,valueOut);

        }
    }

    public static class GroupReducer extends Reducer<Mykey,DoubleWritable,Mykey,DoubleWritable>{

        @Override
        protected void reduce(Mykey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            System.out.print(key+"[");
            for (DoubleWritable tmp: values) {
                System.out.print(tmp+",");
                context.write(key,tmp);
            }
            System.out.println("]");
        }
    }

    public static class GroupRowComParator implements RawComparator<Mykey> {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1,0,l1-8,b2,0,l2-8);
        }



        @Override
        public int compare(Mykey o1, Mykey o2) {
            return 0;
        }
    }

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new GroupMR(),args);
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
        job.setMapOutputKeyClass(Mykey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(GroupMapper.class);


        job.setOutputKeyClass(Mykey.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setReducerClass(GroupReducer.class);

        job.setGroupingComparatorClass(GroupRowComParator.class);

        //4、提交任务
        boolean b = job.waitForCompletion(true);
        return b ?1 :0;
    }
}
