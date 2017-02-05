package com.hadoop.example.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategorySalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String input = value.toString();
		String[] params = input.split("\t");
		if(params.length == 6){
			String category = params[3];
			Double sales = Double.parseDouble(params[4]);
			context.write(new Text(category), new DoubleWritable(sales));
		}
		
	}
	
}
