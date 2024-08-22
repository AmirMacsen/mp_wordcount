package com.amir.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by amir on 11/01/16.
 * Reducer class
 * - 输入的数据
 * -- key: word
 * - 输出的数据
 * -- key: word
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int sum;
    private IntWritable result = new IntWritable();
    // 相同的key为一组，调用一次reduce方法
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // key是相同的，值相加
        sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
