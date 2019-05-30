package flowkeyvalue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * 统计相同手机号的总上行，下行，总流量
 *
 * 		并且按照总流量，手机号，进行排序
 *
 * 		分区
 * 			[0,1G)
 * 			[1G,10G)
 * 			[10G,+oo)
 */

public class PartitionFlowMR extends Configured implements Tool {


    public static class SortFlowMapper extends Mapper<LongWritable, Text,MyKey,NullWritable>{

        MyKey keyOut = new MyKey();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] va = value.toString().split("\t");
            keyOut.setPhone(va[0]);
            keyOut.setUp(Integer.parseInt(va[1]));
            keyOut.setDown(Integer.parseInt(va[2]));
            keyOut.setSum(Integer.parseInt(va[3]));
            context.write(keyOut, NullWritable.get());
        }
    }

    public static class MyPartitioner extends Partitioner<MyKey,NullWritable>{
        @Override
        public int getPartition(MyKey myKey, NullWritable nullWritable, int numPartitions) {

            int sum = myKey.getSum();
            if(sum < 1024){

                return 0;
            }else if(sum < 10 * 1024){

                return  1;
            }
            return 2;
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        // 1 创建job对象
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        // 2 设置输入输出路径
        FileInputFormat.setInputPaths(job, args[0]);
        Path out = new Path(args[1]);
        FileSystem fs = out.getFileSystem(config);
        // 如果存在就删除
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);
        // 3设置map和reduce
        job.setMapOutputKeyClass(MyKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapperClass(SortFlowMapper.class);
        job.setPartitionerClass(MyPartitioner.class);

        job.setNumReduceTasks(3);

        // 4 提交任务
        boolean success = job.waitForCompletion(true);
        return success ? 1 : 0;
    }

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new PartitionFlowMR(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(run==1?"成功":"失败");

    }
}
