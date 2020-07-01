package com.yc.hadoop.project.carSaiesAnalysis.test3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCarReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	
	protected void reduce(Text key,Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		long count = 0L;
		for (LongWritable val : values) {
			count += val.get();
		}
		context.write(key, new LongWritable(count));
	}


	
}
