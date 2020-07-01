package com.yc.hadoop.project.carSaiesAnalysis.test3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// /carSaiesAnalysis/input/cars.txt /carSaiesAnalysis/output/ /carSaiesAnalysis/output2/

public class App {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(null == args || args.length != 3){
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
	
	    Job job1 = Job.getInstance(conf, "统计某个月 各市区县的汽车销售的数量");
	    job1.setJarByClass(App.class);
	    
	    FileInputFormat.addInputPath(job1, input );
	    job1.setMapperClass(CountCarMap.class);
	    job1.setReducerClass(CountCarReducer.class);
	    
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(LongWritable.class);
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(LongWritable.class);
	    
	    FileOutputFormat.setOutputPath(job1, output);
	    
	    job1.waitForCompletion(true);
	    
	    
	    
	    Job job2 = Job.getInstance(conf, "统计某个月 各市的汽车销售的数量");
	    job2.setJarByClass(App.class);
	    
	    Path input2=new Path(args[1]);
	    Path output2=new Path(args[2]);
	    
	    if(fs.exists(output2)){
	    	fs.delete(output2, true);
	    }
	    
	    
	    FileInputFormat.addInputPath(job2, input2);
	    FileOutputFormat.setOutputPath(job2, output2);   
	    
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(LongWritable.class);
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(LongWritable.class);
	    
	    job2.setMapperClass(CountCarMap2.class);
	    job2.setReducerClass(CountCarReducer2.class);
	    
	    job2.waitForCompletion(true);
	    
	}

}
