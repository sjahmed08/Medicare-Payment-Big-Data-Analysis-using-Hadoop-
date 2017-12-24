package heathCareRanking;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.Iterator;
import java.util.*;

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
 * Description: In this program map reduce is used to rank each state by the 
 * average covered charges specific to the DRG charged by providers in 
 * various counties in that state.
 * 
 *
 */

public class stateRankingClass {
	public static String previousDrg = " ";
	boolean firstState = false;
	
	//Tree map for ranking the states by their average Medicare DRG costs
	public static TreeMap<Integer, String> tmapRankingState = new TreeMap<Integer, String>();
	public static HashMap<String, Integer> topStateCount = new HashMap<String, Integer>();
	//Initializing state rank
	public static int stateRank = 1;
	
	public static class drgCountMapper extends Mapper <LongWritable, Text, Text, LongWritable >{
		//declaring variables
		private Text drgDefStateAndCost = new Text();
		private LongWritable stateRanking = new LongWritable();
		private String newDrg = "";
		private int firstKey;
		private int topTenCounter = 1;
		
		//Map function to create key and value
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			//reading input text line and splitting the contents by tab to insert into array
			String[] row = value.toString().split("\t");
			String stateString = "";
			//new capturing DRG number 
			newDrg = row[0].substring(0, 3);
			
			//adding contents of row into treemap in <avgDRGCost, drg_state> order 
			//tree map sorts key value in acending order as it is entered
			if (previousDrg.equals(newDrg) == false)
			{		
				previousDrg = newDrg;

				if (!tmapRankingState.isEmpty()) {
				//mapping highest to lowest states by Average Covered Costs by state
				for (Integer intKey: tmapRankingState.keySet())
				{
					//if (topTenCounter > 40 )
					//{	
						//seperate out state from individual DRG to produce result for top 10 least expensive states
						
						//used for ranking states
						//stateString = tmapRankingState.get(intKey);				
						//stateString = stateString.substring(Math.max(stateString.length() - 2, 0));
						//drgDefStateAndCost.set(stateString);
						
						
						drgDefStateAndCost.set(tmapRankingState.get(intKey) + "  " + intKey);
						
						
						stateRanking.set(stateRank);
						context.write(drgDefStateAndCost, stateRanking);	
						System.out.println(drgDefStateAndCost + " " + stateRanking);
						stateRank += 1;							
					//}
					//else
					//{
					//	topTenCounter += 1;						
					//}
						
				}
				
				/*stateString = tmapRankingState.get(tmapRankingState.firstKey());				
				stateString = stateString.substring(Math.max(stateString.length() - 2, 0));
				System.out.println(stateString + " " + tmapRankingState.firstKey());
				
				if (!topStateCount.containsKey(stateString))
				{
					topStateCount.put(stateString, 1);
				}
				else
				{
					int count = topStateCount.get(stateString);
					topStateCount.put(stateString, count + 1);										
				}
				
				System.out.print("Hash Map Count: " + topStateCount);
				*/
				
				
				//resetting for next DRG entry
				topTenCounter = 0;
				tmapRankingState.clear();
				stateRank = 1;
				tmapRankingState.put(Integer.parseInt(row[1]), row[0]);														
			}
			}
			else
			{
				//inserting key value for individual DRG
				tmapRankingState.put(Integer.parseInt(row[1]), row[0]);
			}					
		}
	}
		
	//reducer function
	public static class drgCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		
		public void reduce(Text Key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			double rankCount = 0; 
			int frequencyCount = 0;
			//summing the received $ amount values by key and outputting the key as DRG+State and value as $dollar
			for (LongWritable val : values)
			{
				rankCount += val.get();
				frequencyCount += 1;			
			}
			if (frequencyCount > 30)
			{
				result.set(frequencyCount);
				context.write(Key, result);
			}
			//output.collect(Key, result);
	}
	}

	public static void main (String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(stateRankingClass.class);
		job.setMapperClass(drgCountMapper.class);
//		job.setCombinerClass(drgCountReducer.class);
//		job.setReducerClass(drgCountReducer.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
//		FileInputFormat.addInputPath(job,  new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath( job, new Path("input"));
		FileOutputFormat.setOutputPath( job, new Path("output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

