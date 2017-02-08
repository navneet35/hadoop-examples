package com.hadoop.example.main;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.example.mapper.StoreCostMapper;
import com.hadoop.example.reducer.StoreCostReducer;

public class StoreCost {
	private static final String DEFAULT_INPUT_PATH = "data/storedata.txt";
	private static final String DEFAULT_OUTPUT_PATH = "joboutput/"+StoreCost.class.getSimpleName()+"Out";

	public static void main(String[] args) throws Exception{
		Job job = Job.getInstance();
		job.setJarByClass(StoreCost.class);
		job.setJobName("Store Cost Job");
		String inputPath = (args.length !=0) ? args[0].trim() : DEFAULT_INPUT_PATH;
		String outputPath = (args.length > 1) ? args[1].trim() : DEFAULT_OUTPUT_PATH;
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(StoreCostMapper.class);
		job.setReducerClass(StoreCostReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		long time = System.currentTimeMillis();
		System.out.println("Execution start at: " + DateFormatUtils.format(time, "dd-MM-yy HH:mm:ss:SS"));
		boolean jobStatus = job.waitForCompletion(true);
		System.out.println("Execution completed in: " + DurationFormatUtils.formatDuration(System.currentTimeMillis() - time, "HH:mm:ss:SS"));
		System.out.println("Job Completed. Job status : " + jobStatus);
		System.exit((jobStatus) ? 0 : 1);
	}

}
