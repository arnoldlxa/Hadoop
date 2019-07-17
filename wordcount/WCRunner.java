package hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCRunner {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		
		Job wcJob = Job.getInstance(conf);
		
		//设置整个job所用 
		wcJob.setJarByClass(WCRunner.class);
		
		//本job使用的mapper和reducer的类
		wcJob.setMapperClass(WCMapper.class);
		wcJob.setReducerClass(WCReducer.class);
		
		//指定输出数据的类型kv类型
		//指定reduce
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(LongWritable.class);
		
		//指定mapper
		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(LongWritable.class);
		
		//指定原书数据
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(wcJob, 
				new Path("hdfs://master:9000/wc/srcdata/"));
		
		//输出数据
		FileOutputFormat.setOutputPath(wcJob, new Path("hdfs://master:9000/wc/output2"));
		
		
		//提交运行
		wcJob.waitForCompletion(true);
		
		
		
		
	}
}
