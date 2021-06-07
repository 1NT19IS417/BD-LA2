// 4.Total number of people who paid tax

package my.bd.pack;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Tax 
{
	 //MAPPER CODE	

	  public static class Map extends MapReduceBase implements Mapper < LongWritable, Text, Text, IntWritable > 
	  {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(LongWritable key, Text value, OutputCollector < Text, IntWritable > output, Reporter reporter) throws IOException 
	    {
	    	String myvalue = value.toString();
	    	String[] tokens = myvalue.split(",");
	    	
	    	if (tokens[4].equals("YES")) {	
	    		output.collect(new Text("Total Tax payers : "), one);
	    	}
	    }
	  }

	  //REDUCER CODE	
	  public static class Reduce extends MapReduceBase implements Reducer < Text, IntWritable, Text, IntWritable > 
	  {
	    public void reduce(Text key, Iterator < IntWritable > values, OutputCollector < Text, IntWritable > output, Reporter reporter) throws IOException 
	    {
	    	int count = 0 ; 
	    	while(values.hasNext()) 
	    	{
	    		count += values.next().get();
	    	}
	    	output.collect(key, new IntWritable(count));

	    }
	  }

	  //DRIVER CODE
	  public static void main(String[] args) throws Exception 
	  {
	    JobConf conf = new JobConf(Tax.class);
	    conf.setJobName("Total no of Tax Paying Employees");
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
