package com.hadoop.example.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.example.mapper.CategorySalesMapper;
import com.hadoop.example.reducer.CategorySalesReducer;

public class CategorySales {
	private static final String DEFAULT_INPUT_PATH = "data/storedata.txt";
	private static final String DEFAULT_OUTPUT_PATH = "joboutput/"+CategorySales.class.getSimpleName()+"Out";

	public static void main(String[] args) throws Exception{
		Job job = Job.getInstance();
		job.setJarByClass(CategorySales.class);
		job.setJobName("Category Sales Job");
		String inputPath = (args.length !=0) ? args[0].trim() : DEFAULT_INPUT_PATH;
		String outputPath = (args.length > 1) ? args[1].trim() : DEFAULT_OUTPUT_PATH;
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(CategorySalesMapper.class);
		job.setReducerClass(CategorySalesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		System.out.println("Execution start");
		boolean jobStatus = job.waitForCompletion(true);
		System.out.println("Job Completed. Job status : " + jobStatus);
		System.exit((jobStatus) ? 0 : 1);
	}

}
