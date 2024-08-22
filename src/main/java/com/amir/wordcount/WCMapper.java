package com.amir.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by amir on 11/03/2017.
 * - 输入数据
 * -- LongWritable: 输入数据的偏移量
 * -- Text: 输入数据
 * - 输出数据
 * -- Text: 输出数据的key
 * -- IntWritable: 输出数据的value
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 输出数据的value，默认为1，设置成静态变量，避免多次创建
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // 文本中的每一行调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        if (value != null) {
            // 读取到的当前行的内容，转化成字符串
            String line = value.toString();
            // 将当前行按空格分割成多个单词
            String[] words = line.trim().split(" ");
            // 遍历单词数组，输出单词和1
            for (String w : words) {
                if (w.trim().isEmpty()) {
                    continue;
                }
                word.set(w);
                context.write(word, one);
            }
        }
    }
}
