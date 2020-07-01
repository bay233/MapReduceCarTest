package com.yc.hadoop.project.carSaiesAnalysis.test6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCarMap extends Mapper<Object, Text, Text, LongWritable> {

	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().trim().split(",");
		if(arr.length > 12 && !"".equals(arr[1]) && arr[1] != null && Integer.parseInt(arr[1]) == 9 && arr[7] != null ){
			context.write(new Text(arr[1] +"    "+ arr[7]), new LongWritable(1));
		}
	}
}
