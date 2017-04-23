package com.edureka.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.edureka.mapper.WordCountMapper;
import com.edureka.reducer.WordCountReducer;

//import com.edureka.mapper.WordCountMapper;
//import com.edureka.reducer.WordCountReducer;

public class WordCountJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		
		//validating input
		
		if(args.length<2)
		{
			System.out.println("Java Usage: "+WordCountJob.class.getName()+" /path/to/input /path/to/output");
			return;
		}
		
		Configuration conf=new Configuration(Boolean.TRUE);
		
		try {
			int i=ToolRunner.run(conf, new WordCountJob(), args);
			
			if(i==0){
				System.out.println("SUCESS");
			}else{
				System.out.println("Failed");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	@Override
	public int run(String[] args) throws Exception 
	{
		
		//step-1 : Load Configuration
		Configuration conf=super.getConf();
		//step -2 : Create job instance
		//Set any thing particular to mapreduce by developer goes here
		Job wordCountJob=Job.getInstance(conf, WordCountJob.class.getName());

		//setting classpath
		wordCountJob.setJarByClass(WordCountJob.class); 
		
		//setting input
		final String input=args[0];
		final Path inputPath=new Path(input); //hadoop understand path
		TextInputFormat.addInputPath(wordCountJob, inputPath);
		wordCountJob.setInputFormatClass(TextInputFormat.class);

		
		//setting output
		final String output=args[1];
		final Path outputPath=new Path(output);
		TextOutputFormat.setOutputPath(wordCountJob, outputPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		//setting mapper
		wordCountJob.setMapperClass(WordCountMapper.class);
		//TODO
//		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		
		//setting reducer
		wordCountJob.setReducerClass(WordCountReducer.class);
		
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(LongWritable.class);
		
//		wordCountJob.setNumReduceTasks(3);
		
		wordCountJob.waitForCompletion(Boolean.TRUE);
		
		return 0;
	}

}
