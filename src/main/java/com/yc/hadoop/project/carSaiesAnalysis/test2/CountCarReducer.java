package com.yc.hadoop.project.carSaiesAnalysis.test2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCarReducer extends Reducer<IntWritable, LongWritable, IntWritable, Text> {
	Map<Integer, Long> map = new HashMap<>();
	double all = 0;
	
	
	protected void reduce(IntWritable key,Iterable<LongWritable> values, Context context){
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		all += sum;
		map.put(key.get(), sum);
	}


	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		Set <Integer> keySet = map.keySet();
		for(Integer key:keySet){
			long value = map.get(key);
			double percent = value/all;
			context.write(new IntWritable(key), new Text(value+"\t"+percent));
		}
	}
	
	
}
