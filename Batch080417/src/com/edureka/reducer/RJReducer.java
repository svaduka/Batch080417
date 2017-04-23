package com.edureka.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RJReducer extends
		Reducer<Text, Text, Text, Text> {


	@Override
	protected void reduce(Text key,Iterable<Text> values,Context context) throws java.io.IOException, InterruptedException 
	{
		
		//40000001
		//{txn	040.35, txn	040.35,txn	040.35,txn	040.35,txn	040.35,txn	040.35,custs	kristina,txn	040.35,txn	040.35,txn	040.35,txn	040.35,txn	040.35,}

		String name = "";
		double total = 0.0;
		int count = 0;
		for (Text t : values) {
			String parts[] = t.toString().split("\t");
			if (parts[0].equals("txns")) {
				count++;
				total += Float.parseFloat(parts[1]);
			} else if (parts[0].equals("custs")) {
				name = parts[1];
			}
		}
		String str = String.format("%d\t%f", count, total);
		context.write(new Text(name), new Text(str));
		
	};

}
