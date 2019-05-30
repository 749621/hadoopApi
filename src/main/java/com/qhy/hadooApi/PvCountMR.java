package com.qhy.hadooApi;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 *
 * Mapper:
 *      key     in： 偏移量     LonGWritable
 *      value   in： 一件文本   Text
 *      key     out：省份id    Text
 *      value   out：1        IntWritable
 *
 *      将省份id作为key，将1作为value
 *
 * Reduce
 *
 *         key     in   省份id                                    Text
 *         value   in   包含所有身份出现次数的集合list(1,1,1,1,1)    IntWritable
 *         key     out  省份id                                    Text
 *         value   out  省份pv量                                  IntWritable
 */

public class PvCountMR extends Configured implements Tool {

    public static class  PVMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();
        final String DIRTY_DATA = "DIRTY_DATA";
        final String URL_EMPTY  = "URL_IS_BLANK";
        final String PROVINCEID_NOT_NUM = "PROVIENCEID_NOT_NUM";
        final String COLUMN_PRO = "COLUMN UNDER 23";


        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取省份id
            String[] vals = value.toString().split("\t");
            //字段不足
            if(vals.length < 24){

                context.getCounter(DIRTY_DATA,COLUMN_PRO).increment(1L);
                return;
            }
            String provinceld = vals[23];
            String url = vals[1];
            if(StringUtils.isBlank(url)){
                context.getCounter(DIRTY_DATA,URL_EMPTY).increment(1L);

                return;
            }

            //省份id乱吗
            try {
                Integer.parseInt(provinceld);
            } catch (NumberFormatException e) {
                context.getCounter(DIRTY_DATA,PROVINCEID_NOT_NUM).increment(1L);
                return;
            }
            //包装key value ，并输出
            keyOut.set(provinceld);
            valueOut.set(1);
            context.write(keyOut,valueOut);


        }

    }


    public static class PVReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        IntWritable valueOut = new IntWritable();
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

                //  求出省份pv的he
            int sum = 0;
            for (IntWritable tmp : values){

                sum+=tmp.get();
            }
            valueOut.set(sum);
            context.write(key,valueOut);

        }
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
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(PVMapper.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(PVReducer.class);

        job.setNumReduceTasks(3);

        //4、提交任务
        boolean b = job.waitForCompletion(true);
        return b ?1 :0;
    }

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(configuration,new PvCountMR(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(run > 0?"成功":"失败");
    }
}
