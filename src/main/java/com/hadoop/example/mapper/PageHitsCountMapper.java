package com.hadoop.example.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Log File is given: 
 * Count the page hits in the log. 
 * Example format 
 * 10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET /assets/js/the-associates.js HTTP/1.1" 200 4492
 * 10.223.157.186 - - [15/Jul/2009:15:50:51 -0700] "GET /assets/js/the-associates.js HTTP/1.1" 304 -
 * */

public class PageHitsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final IntWritable ONE = new IntWritable(1);
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String input = value.toString();
		String[] params = input.split(" ");
		if(params.length == 10){
			String page = params[6];
			context.write(new Text(page), ONE);
		}
		
	}
	
}
