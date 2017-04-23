package com.edureka.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TxnMapper extends
		Mapper<LongWritable, Text, Text, Text> {


	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException 
	{
		
		//key --0
		//value --00000000,06-26-2011,4000001,040.33,Exercise & Fitness,Cardio Machine Accessories,Clarksville,Tennessee,credit
		
		
		String input = value.toString();
		if(!StringUtils.isEmpty(input))
		{
		
		String[] tokens = StringUtils.splitPreserveAllTokens(input, ",");
		
		context.write(new Text(tokens[2]), new Text("txns\t" + tokens[3]));
		}
	};
}
