package com.hadoop.example.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageHitsCountReducer extends Reducer<Text, IntWritable, Text, LongWritable>{

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Long hits = 0L;
		for(IntWritable val : values){
			hits += val.get();
		}
		context.write(key, new LongWritable(hits));
	}

}
