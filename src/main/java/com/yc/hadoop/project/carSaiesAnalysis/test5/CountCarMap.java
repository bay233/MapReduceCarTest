package com.yc.hadoop.project.carSaiesAnalysis.test5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCarMap extends Mapper<Object, Text, Text, LongWritable> {

	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] strs = value.toString().trim().split(",");
		if (strs != null && strs.length > 38 && strs[5] != null && strs[8] != null && strs[37] != null
				&& strs[37].matches("^\\d*$") && strs[38] != null) {
			int age = Integer.parseInt(strs[4]) - Integer.parseInt(strs[37]);
			int rangel = age / 10*10;
			int range2 = rangel + 10;
			context.write(new Text(strs[8] + "," + (rangel + "-" + range2) + ","+strs[38]), new LongWritable(1));
		}
	}
}
