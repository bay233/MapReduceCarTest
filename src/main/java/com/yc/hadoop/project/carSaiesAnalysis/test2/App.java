package com.yc.hadoop.project.carSaiesAnalysis.test2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yc.hadoop.project.carSaiesAnalysis.test2.App;
import com.yc.hadoop.project.carSaiesAnalysis.test2.CountCarCombiner;
import com.yc.hadoop.project.carSaiesAnalysis.test2.CountCarMap;
import com.yc.hadoop.project.carSaiesAnalysis.test2.CountCarReducer;

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
	
	    Job job = Job.getInstance(conf, "统计每月销售比例");
	    job.setJarByClass(App.class);
	    
	    FileInputFormat.addInputPath(job, input );
	    job.setMapperClass(CountCarMap.class);
	    job.setCombinerClass(CountCarCombiner.class);
	    job.setReducerClass(CountCarReducer.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileOutputFormat.setOutputPath(job, output);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
