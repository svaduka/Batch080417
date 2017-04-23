package com.edureka.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMapper extends
		Mapper<LongWritable, Text, Text, Text> {


	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException 
	
	{
		//key -- 0
		//value -- 4000001,Kristina,Chung,55,Pilot
		
		String input = value.toString();
		
		if(!StringUtils.isEmpty(input))
		{
		String[] tokens = StringUtils.splitPreserveAllTokens(input, ",");
		
		context.write(new Text(tokens[0]), new Text("custs\t" + tokens[1]));
		
		}
	};
}
