package wc.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MyWcPartition extends Configured implements Tool {

    public static class MyWcMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            context.write(new Text(split[0]),new IntWritable(Integer.parseInt(split[1])));
        }
    }

    //自定义分区  将单词出现次数>1和=1分开
    public static class WCPartition extends Partitioner<Text,IntWritable> {

        /**
         *
         * @param key               map输出的key
         * @param value             map输出的value
         * @param numPartitions     分区个数
         * @return                  必须从0开始的 连续的值
         */
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {


            int count = value.get();

            if(count > 1){
                return 1;
            }
            return 0;
        }

    }

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new MyWcPartition(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(run > 0?"成功":"失败");
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //1、创建Job对象
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(WordCount.class);
        //2、设置输入输出路径
        FileInputFormat.setInputPaths(job, args[0]);
        Path path = new Path(args[1]);
        FileSystem fs = path.getFileSystem(conf);
        //3、如果存在就删除
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
        //3、设置map和reduce对象
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyWcMapper.class);


        //设置分区个数
        job.setNumReduceTasks(2);

        //设置分区类
        job.setPartitionerClass(WCPartition.class);

        //4、提交任务
        boolean b = job.waitForCompletion(true);
        return b ? 1 : 0;
    }
}
