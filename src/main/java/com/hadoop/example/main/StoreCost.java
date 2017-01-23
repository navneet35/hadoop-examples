package com.hadoop.example.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.example.mapper.StoreCostMapper;
import com.hadoop.example.reducer.StoreCostReducer;

public class StoreCost {

	public static void main(String[] args) throws Exception{
		Job job = Job.getInstance();
		job.setJarByClass(StoreCost.class);
		job.setJobName("Store Cost Job");
		FileInputFormat.addInputPath(job, new Path("data/storedata.txt"));
		FileOutputFormat.setOutputPath(job, new Path("joboutput/"+StoreCost.class.getSimpleName()+"Out"));
		job.setMapperClass(StoreCostMapper.class);
		job.setReducerClass(StoreCostReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		System.out.println("Execution start");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
