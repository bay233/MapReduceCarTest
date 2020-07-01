package com.yc.hadoop.project.carSaiesAnalysis.test3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCarCombiner extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

	private LongWritable result = new LongWritable();
	
	public void reduc(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		Long sum = new Long(0);
		for (LongWritable val : values){
			sum += val.get();
		}
		result.set(sum);
		context.write(key , result);
	}
}
