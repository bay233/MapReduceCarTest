package com.yc.hadoop.project.carSaiesAnalysis.test3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCarMap extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString().trim();
		String[] arr = line.split(",");
		if (arr != null && arr.length == 39 && arr[1].equals("4")){
			String cityAndCounty = arr[2] + "," + arr[3];
			context.write(new Text(cityAndCounty), new LongWritable(Long.parseLong(arr[11])));
		}
		
	}
	
	
}
