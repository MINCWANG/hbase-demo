package com.wmc.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author: WangMC
 * @date: 2019/9/25 21:44
 * @description:
 */
public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {

    // Configuration configuration;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // configuration = context.getConfiguration();
        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //1、遍历 1001	Apple	Red
        for (Text value : values) {
            //2、获取每一行数据
            String[] fileds = value.toString().split("\t");
            //3、构建Put对象
            Put put = new Put(Bytes.toBytes(fileds[0]));
            //4、给Put对象赋值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fileds[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fileds[2]));
            //5、写出
            context.write(NullWritable.get(), put);
        }
    }
}
