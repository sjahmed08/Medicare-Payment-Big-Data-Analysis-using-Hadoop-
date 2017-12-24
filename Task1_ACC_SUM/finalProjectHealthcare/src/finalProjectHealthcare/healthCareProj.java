package finalProjectHealthcare;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**********************************************************************************
 * 
 * 
 * 
 * @author Syed J. Ahmed
 * Program: State Ranking Class 
 * Version: 1.0
 * Title: CIS 5570 Introduction to Big Data Final Project
 * Date: 12/05/2017
 * 
 * Description: In this program map reduce is used to find the average covered
 * cost for each state by looking into different counties and regions in each 
 * state and sampling their recorded costs. The data file that is used contains
 * 168,000 records for various DRG (Diagnosis Related Group) from various 
 * providers in each state.
 * 
 * 
 */

public class healthCareProj {

//Mapper class 
public static class drgCountMapper extends Mapper <LongWritable, Text, Text, LongWritable>{
	
	private Text drgDefinitionAndState = new Text();
	private LongWritable AverageCoveredCharges = new LongWritable();
		//Map function to create key and value
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//OutputCollector<Text, IntWritable> output
		//Creating String array to split each line/row by tabs
		String[] row = value.toString().split("\t");
		
		//check the size of row to ensure there is data
		int sizeofRow = row.length;
		if (sizeofRow > 1)
		{	
			
			//from the row array taking 0th element which is the DRG classification
			String conditionMed = row[0];
			
			//from the row array taking the 5th element which is the state			
			String providerState = row[5];
			
			//from the row array taking the 9th element which is the Average Medical Cost			
			String medCost = row[9];
			
			try {
				medCost = medCost.substring(0, medCost.indexOf("."));
			}
			catch (StringIndexOutOfBoundsException e)
			{
				System.out.print("OutofBound Exception Occured ------" + medCost);
			}
						
			
			//removing commas and other signs in value
			medCost = medCost.replaceAll("[$,.]", "");
			System.out.println(medCost);
			//sending DRG classification and provider state as key and AverageCovered Charges as key
			//sending these variables to the reduce function
			drgDefinitionAndState.set(conditionMed + "---" + providerState);
			AverageCoveredCharges.set(Long.parseLong(medCost));
			
			
			//Mapper output
			context.write(drgDefinitionAndState, AverageCoveredCharges);
		}	
	}
}

//Reducer function
public static class drgCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	private LongWritable result = new LongWritable();
	
	public void reduce(Text Key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		//OutputCollector<Text, IntWritable> output
		double sumDecimal = 0; 
		int sumInteger = 0;
		int avgCount = 0;
		//summing the received $ amount values by key and outputting the key as DRG+State and value as $dollar
		for (LongWritable val : values)
		{
			sumDecimal += val.get();
			avgCount += 1;
		}
	
		sumInteger = (int) (sumDecimal);
		sumInteger = (sumInteger / avgCount);
		//System.out.println("int sum " + sumInteger);
		result.set(sumInteger);
		context.write(Key, result);
	}
}

public static void main (String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "word count");
	job.setJarByClass(healthCareProj.class);
	job.setMapperClass(drgCountMapper.class);
//	job.setCombinerClass(drgCountReducer.class);
	job.setReducerClass(drgCountReducer.class);

	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
//	FileInputFormat.addInputPath(job,  new Path(args[0]));
//	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	FileInputFormat.addInputPath( job, new Path("input"));
	FileOutputFormat.setOutputPath( job, new Path("output"));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
