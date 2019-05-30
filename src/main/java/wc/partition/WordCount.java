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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import java.io.IOException;



/**
 * JAVA序列化接口：            Servializable
 *
 * Hadoop的序列化接口：        Writable     WritableComparable
 *  long        Long         LogWritable
 *  int         Integer      IntWritable
 *  double      Double       DoubleWritable
 *              String       Text
 *              NULL         NullWritable
 *
 *
 *
 * MapperReduce程序：
 * 分：
 *  Mapper（Map阶段）:
 *      KEYIN:    偏移量                        LongWritable
 *      VALUEIN:  一行文本内容                  Text
 *      KEYOUT:   单词                        Text
 *      VALUEOUT: 单词出现的次数               IntWritable
 *
 *      map();
 *
 *  Reducer（Reduce阶段）
 *      KEYIN:      单词                     Text
 *      VALUEIN:    存有单词次数的集合        IntWritable
 *      KEYOUT:     单词                   Text
 *      VALUEOUT:   单词出现的总次数        IntWritable
 *      reduce();
 */
public class WordCount extends Configured implements Tool {




    public static class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {

        /**
         *
         * @param key       单词
         * @param value     一行文本
         * @param context   上下文对象
         * @throws IOException
         * @throws InterruptedException
         */
        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //1、分割一行文本为字符串数组
            String[] vals = value.toString().split(" ");
            //2、获取 key 和value
            for (String map : vals){
                    keyOut.set(map);
                    valueOut.set(1);
                //3、写出 key value
                context.write(keyOut,valueOut);
                
            }


        }
    }


    public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        IntWritable valueOut = new IntWritable();
        /**
         *
         * @param key       单词
         * @param values    存有单词出现的集合
         * @param context   上下文对象
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            //  1、累加求和
            int sum = 0 ;
            for (IntWritable temp : values){

                sum += temp.get();
            }

            //2、包装key、value 并输出
            valueOut.set(sum);
            context.write(key,valueOut);
        }
    }

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new WordCount(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(run > 0?"成功":"失败");
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //1、创建Job对象
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(WordCount.class);
        //2、设置输入输出路径
        FileInputFormat.setInputPaths(job,args[0]);
        Path path = new Path(args[1]);
        FileSystem fs = path.getFileSystem(conf);
        //3、如果存在就删除
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //3、设置map和reduce对象
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(WordCountMapper.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(WordCountReducer.class);

        //设置分区个数
        job.setNumReduceTasks(1);

        //4、提交任务
        boolean b = job.waitForCompletion(true);
        return b ?1 :0;
    }

}
