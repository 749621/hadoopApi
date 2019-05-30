package flowkeyvalue;


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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FlowMR extends Configured implements Tool {


    /**
     * KEYIN    偏移量
     * VALUEIN  一行
     * KEYOUT   手机号         Text
     * VALUEOUT 自定义类型（up，down，sum）
     */
    public static class FlowMapper extends Mapper<LongWritable, Text,Text,Flow>{

        Text keyOut = new Text();
        Flow valueOut = new Flow();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取手机号 上行 下行
            String[] values = value.toString().split("\t");
            //手机号
            String num = values[1];
            long up=0;//上行
            long down=0;//下行
            try {
                    up = Long.parseLong(values[8]);
                    down = Long.parseLong(values[9]);
            } catch (NumberFormatException e) {

                context.getCounter("dirty_data","up and down not num").increment(1L);
                return;
            }

            //封装key value
            keyOut.set(num);
            valueOut.setUp(up);
            valueOut.setDown(down);
            context.write(keyOut,valueOut);
        }
    }


    /**
     * shuffle：
     * 		group :<手机号：list（value(,,),value(,,)...）>
     *
     *
     * KEYIN
     * VALUEIN
     * KEYOUT   手机号         Text
     * VALUEOUT 自定义类型（up，down，sum）
     */
    public static class FlowReducer extends Reducer<Text,Flow,Text,Flow>{

        Flow valueOut = new Flow();

        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {

            //求出总上行和下行
            long up = 0;
            long down = 0;

            for (Flow tmp : values){

                up += tmp.getUp();
                down += tmp.getDown();

            }

            //封装 key value
            valueOut.setUp(up);
            valueOut.setDown(down);
            valueOut.setSum(up+down);
            //输出
            context.write(key,valueOut);


        }
    }

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new FlowMR(),args);
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
        job.setMapOutputValueClass(Flow.class);
        job.setMapperClass(FlowMapper.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);
        job.setReducerClass(FlowReducer.class);

        //4、提交任务
        boolean b = job.waitForCompletion(true);
        return b ?1 :0;
    }
}
