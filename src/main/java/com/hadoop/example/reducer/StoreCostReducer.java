package com.hadoop.example.reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StoreCostReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{

	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		Float cost = 0.0F;
		
		for(FloatWritable val : values){
			cost = Float.sum(cost, val.get());
		}
		
		context.write(key, new FloatWritable(cost));
	}

}
