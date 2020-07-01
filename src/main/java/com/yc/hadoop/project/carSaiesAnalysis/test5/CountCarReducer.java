package com.yc.hadoop.project.carSaiesAnalysis.test5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCarReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		context.write(key, new LongWritable(sum));
	}

}
