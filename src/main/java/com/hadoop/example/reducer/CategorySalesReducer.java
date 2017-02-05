package com.hadoop.example.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CategorySalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		Double cost = 0.0d;
		
//		if(key.equals("Toys") || key.equals("Consumer Electronics")){
			for(DoubleWritable val : values){
				cost = Double.sum(cost, val.get());
			}
			context.write(key, new DoubleWritable(cost));
//		}
		
	}

}
