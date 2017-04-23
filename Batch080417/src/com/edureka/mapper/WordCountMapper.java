package com.edureka.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key,Text value,Context context) throws java.io.IOException, InterruptedException 
	{
		
		//key --- 0  --- LONGWRITABLE
			//value ---DEAR BEAR RIVER --- TEXT
		
		final String input=value.toString();
		
		if(!StringUtils.isEmpty(input))
		{
			final String[] words = StringUtils.splitPreserveAllTokens(input, " ");
			
			IntWritable ONE = new IntWritable(1);
			
			for (String word : words) 
			{
				context.write(new Text(word), ONE);
			}
		}
		
	};


}
