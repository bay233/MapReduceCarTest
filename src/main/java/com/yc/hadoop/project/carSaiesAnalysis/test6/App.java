package com.yc.hadoop.project.carSaiesAnalysis.test6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// /carSaiesAnalysis/input/cars.txt /carSaiesAnalysis/output/
public class App {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(null == args || args.length != 2){
			System.out.println("<Usage> Please <input> <output>");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		
		//注意是处于active状态的 HDFS
		conf.set("fs.defaultFS", "hdfs://bay1:8020");
		
		Path input=new Path(args[0]);
	    Path output=new Path(args[1]);
	    
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(output)){
	    	fs.delete(output, true);
	    }
	
	    Job job = Job.getInstance(conf, "统计车年龄比例的销量比重");
	    job.setJarByClass(App.class);
	    
	    FileInputFormat.addInputPath(job, input );
	    job.setMapperClass(CountCarMap.class);
	    job.setReducerClass(CountCarReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    FileOutputFormat.setOutputPath(job, output);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
