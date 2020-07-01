package com.yc.hadoop.project.carSaiesAnalysis.test2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCarMap extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
	private IntWritable month = new IntWritable();
	private LongWritable num = new LongWritable();
	
	protected void map(LongWritable key, Text value, Context context){
		
		String line = value.toString();
		String[] arr = line.split(",");
		if (arr.length > 11 && null != arr[11] && !"".equals(arr[11].trim())){
			try{
				month.set(Integer.parseInt(arr[1]));
				num.set(Long.parseLong(arr[11]));
				context.write(month, num);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
	}
	
	
}
