package com.wmc.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: WangMC
 * @date: 2019/9/25 22:00
 * @description:
 */
public class FruitDriver implements Tool {

    /**
     * 定义一个configuration
     */
    Configuration configuration = null;

    @Override
    public int run(String[] args) throws Exception {
        //1、获取Job对象
        Job job = Job.getInstance(configuration);
        //2、设置驱动类路径
        job.setJarByClass(FruitDriver.class);

        //3、设置Mapper & Mapper输出的KV类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //4、设置Reducer类
        TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class, job);
        //5、设置最终输出数据的KV类型

        //6、设置输入输出参数
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //7、提交任务
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    @Override
    public Configuration getConf() {

        return configuration;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }


    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            int run = ToolRunner.run(configuration, new FruitDriver(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
