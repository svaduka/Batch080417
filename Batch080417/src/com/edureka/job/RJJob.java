package com.edureka.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.edureka.mapper.CustomerMapper;
import com.edureka.mapper.TxnMapper;
import com.edureka.reducer.RJReducer;


public class RJJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		if(args.length<3)
		{
			System.out.println("Java Usage: "+RJJob.class.getName()+" /path/to/input/cust /path/to/input/txn /path/to/output");
			return;
		}
		
		Configuration conf=new Configuration(Boolean.TRUE);
		
		try {
			int i=ToolRunner.run(conf, new RJJob(), args);
			
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
		
		Configuration conf=super.getConf();
		//Set any thing particular to mapreduce by developer goes here
		Job rjJob=Job.getInstance(conf, RJJob.class.getName());

		//setting classpath
		rjJob.setJarByClass(RJJob.class); 
		
		
		
		//setting  multiple input
		
		
		//setting  customer input
		final String custInput=args[0];
		final Path custInputPath=new Path(custInput); //hadoop understand path
		
		MultipleInputs.addInputPath(rjJob, custInputPath, TextInputFormat.class, CustomerMapper.class);
		
		//setting  txn input
		final String txnInput=args[1];
		final Path txnInputPath=new Path(txnInput); //hadoop understand path
		
		MultipleInputs.addInputPath(rjJob, txnInputPath, TextInputFormat.class, TxnMapper.class);
		
				
		//setting output
		final String output=args[2];
		final Path outputPath=new Path(output);
		TextOutputFormat.setOutputPath(rjJob, outputPath);
		rjJob.setOutputFormatClass(TextOutputFormat.class);
		
		
		//setting reducer
		rjJob.setReducerClass(RJReducer.class);
		
		rjJob.setOutputKeyClass(Text.class);
		rjJob.setOutputValueClass(Text.class);
		
		rjJob.waitForCompletion(Boolean.TRUE);
		
		return 0;
	}

}
