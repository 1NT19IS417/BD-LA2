// 1.Calculate total number eligible for pay for a Bonus 

package my.bd.pack;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class EligibleRaise 
{
	//MAPPER CODE	
	   
			public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
			{
			private final static IntWritable one = new IntWritable(1);
			private Text word = new Text();

			public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
			{
				String myvalue = value.toString();
				String[] wordtokens = myvalue.split(",");
				
				if (wordtokens[5].equals("YES")) 
				{	
		    		output.collect(new Text("No of People Eligible for a Raise are : "), one);
		    	}
				
				}
			}

			//REDUCER CODE	
			public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
			{
			public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
			{ 
				int elcount = 0 ; 
		    	while(values.hasNext()) {
		    		elcount += values.next().get();
		    	}
		    	output.collect(key, new IntWritable(elcount));

				}
			}
				
			//DRIVER CODE
			public static void main(String[] args) throws Exception 
			{
				JobConf conf = new JobConf(EligibleRaise.class);
				conf.setJobName("To check for Eligible Raise");
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(IntWritable.class);
				conf.setMapperClass(Map.class);
				conf.setCombinerClass(Reduce.class);
				conf.setReducerClass(Reduce.class);
				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class); 
				FileInputFormat.setInputPaths(conf, new Path(args[0]));
				FileOutputFormat.setOutputPath(conf, new Path(args[1]));
				JobClient.runJob(conf);   
			}
}
