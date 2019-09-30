package com.wmc.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: WangMC
 * @date: 2019/9/25 21:41
 * @description:
 */
public class FruitMapper  extends Mapper<LongWritable, Text,LongWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.write(key, value);
    }
}
