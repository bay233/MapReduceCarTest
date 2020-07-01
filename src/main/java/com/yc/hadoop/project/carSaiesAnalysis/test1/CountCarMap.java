package com.yc.hadoop.project.carSaiesAnalysis.test1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCarMap extends Mapper<Object, Text, Text, LongWritable> {
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String[] strs = value.toString().trim().split(",");
		if (strs!=null && strs.length > 10 && strs[10] != null){
			String type = strs[10];
			if ("非营运".equals(type)){
				context.write( new Text("乘用车辆"),new LongWritable(1));
			}else {
				context.write(new Text("商用车辆"), new LongWritable(1));
			}
		}
	}
}
