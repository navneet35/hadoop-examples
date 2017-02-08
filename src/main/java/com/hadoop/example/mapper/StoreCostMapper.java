package com.hadoop.example.mapper;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreCostMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
    private static final Logger LOGGER = LoggerFactory.getLogger(StoreCostMapper.class);

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] input = line.split("\t");
		if(input.length == 6){
			Float cost = Float.parseFloat(input[4]);
			String store = input[2];
			context.write(new Text(store), new FloatWritable(cost));
		}

	}

}
