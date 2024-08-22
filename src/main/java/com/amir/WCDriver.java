package com.amir;


import com.amir.wordcount.WCMapper;
import com.amir.wordcount.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 入口程序
 * 在这个类中，添加Job 的配置，设置输入输出路径并提交Job
 *
 */
public class WCDriver
{
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException {
        // 0.输入参数校验
        if (args.length != 2) {
            System.out.println("Usage: hadoop jar xxx.jar com.amir.WCDriver <input path> <output path>");
            System.exit(2);
        }
        System.setProperty("HADOOP_USER_NAME", "root");
        // 1.创建配置文件对象
        Configuration conf = new Configuration();
        // 2.设置本地运行
        conf.set("mapreduce.framework.name", "local");

        // 设置本地目录
        conf.set("mapreduce.cluster.local.dir", "/Users/xxx/workspace/javaprojects/mp_wordcount/mp_wordcount/src/main/resources\n");

        // 3.创建作业对象
        Job job = Job.getInstance(conf);
        // 6.设置driver类
        job.setJarByClass(WCDriver.class);
        // 7.设置mapper和reducer类
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 8.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 4.提交作业
        boolean b = job.waitForCompletion(true);
        // 5.根据返回结果退出程序
        System.exit(b ? 0 : 1);
    }
}
