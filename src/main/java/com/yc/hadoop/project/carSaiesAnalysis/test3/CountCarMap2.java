package com.yc.hadoop.project.carSaiesAnalysis.test3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCarMap2 extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] str = value.toString().trim().split("\t");
		if (str != null && str.length > 1){
			String[] city = str[0].split(",");
			if (city != null && city.length > 0){
				context.write(new Text(city[0]), new LongWritable(Long.parseLong(str[1])));
			}
		}
		
	}
	
	
}
