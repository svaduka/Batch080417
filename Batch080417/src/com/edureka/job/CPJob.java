package com.edureka.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.edureka.mapper.WordCountMapper;
import com.edureka.partitioner.MyPartitioner;
import com.edureka.reducer.WordCountReducer;

//import com.edureka.mapper.WordCountMapper;
//import com.edureka.reducer.WordCountReducer;

public class CPJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		//step : 1 Validate input arguments
		if(args.length<2)
		{
			System.out.println("JAVA USAGE "+CPJob.class.getName()+" [configuration] /hdfs/input/path /hdfs/output/path");
			return;
		}
		
		System.out.println("IN MAIN METHOD");
		
		//Load the configuration
		Configuration conf=new Configuration(Boolean.TRUE);
		
		try {
			//Use ToolRunner for generic option parser parsing command arguments and set to the configuration
			int i=ToolRunner.run(conf, new CPJob(), args);
			if(i==0){
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception 
	{
		System.out.println("IN RUN METHOD");
		
		//Get the configuration from the super class which was populated by ToolRunner.run
		Configuration conf=super.getConf();
		
		//Create a Job Instance
		Job cpJob=Job.getInstance(conf, CPJob.class.getName());
		
		//set the ClassPath
		cpJob.setJarByClass(CPJob.class);
		
		
		
		//Setting Input
		final String input=args[0];
		final Path inputPath=new Path(input);
		TextInputFormat.addInputPath(cpJob, inputPath);
		cpJob.setInputFormatClass(TextInputFormat.class);
		
		//setting output
		final String output=args[1];
		final Path outputPath=new Path(output);
		TextOutputFormat.setOutputPath(cpJob, outputPath);
		cpJob.setOutputFormatClass(TextOutputFormat.class);

		cpJob.setMapperClass(WordCountMapper.class);
		//Explicitly specify the type of data emits from the mapper
		cpJob.setMapOutputKeyClass(Text.class);
		cpJob.setMapOutputValueClass(IntWritable.class);
		
		cpJob.setCombinerClass(WordCountReducer.class);
		cpJob.setPartitionerClass(MyPartitioner.class);
		cpJob.setNumReduceTasks(3);
		
		
		cpJob.setReducerClass(WordCountReducer.class);
		
		cpJob.setOutputKeyClass(Text.class);
		cpJob.setOutputValueClass(IntWritable.class);
		
		cpJob.waitForCompletion(Boolean.TRUE);
		
		
		
		return 0;
	}
}
