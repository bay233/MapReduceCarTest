package com.yc.hadoop.project.carSaiesAnalysis.test4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCarMap extends Mapper<Object, Text, Text, LongWritable> {
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String[] strs = value.toString().trim().split(",");
		if (strs!=null && strs.length > 38 && strs[38] != null){
			if ("男性".equals(strs[38]) || "女性".equals(strs[38])){
				context.write(new Text(strs[38]), new LongWritable(1));
			}
		}
	}
}
