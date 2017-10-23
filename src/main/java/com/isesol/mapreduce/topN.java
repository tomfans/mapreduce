package com.isesol.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.net.nntp.NewsgroupInfo;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class topN {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private SortedMap top10 = new TreeMap();
		String topn = "10"; 

		protected void setup(Context context) throws IOException, InterruptedException {
			// NOTHING
			//topn = Integer.parseInt(context.getConfiguration().get("topn")); 	
			topn = context.getConfiguration().get("topn");
			if(topn == null) {
				topn = "10";
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String str = "";
			String[] val = value.toString().split(";");
			for (int i = 1; i < val.length - 1; i++) {
				str += val[i];
			}
			top10.put(val[0], str);

			if (top10.size() > Integer.parseInt(topn)) {
				top10.remove(top10.firstKey());
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

			Iterator iterator = top10.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry ent = (Map.Entry) iterator.next();
				String key = ent.getKey().toString();
				String value = ent.getValue().toString();

				context.write(new Text(key), new Text(value));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		String topn = "10";
		private SortedMap top10 = new TreeMap();

		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

			for (Text val : value) {
				top10.put(key.toString(), val.toString());
			}

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

			topn = context.getConfiguration().get("topn");
			
			topn = context.getConfiguration().get("topn");
			if(topn == null) {
				topn = "10";
			}
			
			System.out.println("top10 size isa: " + top10.size());

			while (top10.size() > Integer.parseInt(topn)) {
				top10.remove(top10.firstKey());
			}

			Iterator iterator = top10.entrySet().iterator();

			System.out.println("top10 size is: " + top10.size());

			while (iterator.hasNext()) {
				Map.Entry ent = (Map.Entry) iterator.next();
				String key01 = ent.getKey().toString();
				String value01 = ent.getValue().toString();
				context.write(new Text(key01), new Text(value01));
			}

		}

	}

	public static void main(String[] args) throws Exception {
		
		if(args.length >2) {
			Configuration conf = new Configuration();
			conf.set("topn", args[2]);
			Job job = Job.getInstance(conf, "topN");
			job.setJarByClass(topN.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			// job.setPartitionerClass(twopartitions.class);
			// job.setOutputFormatClass(fakeOutPutFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} else {
			Configuration conf = new Configuration();
		//	conf.set("topn", args[2]);
			Job job = Job.getInstance(conf, "topN");
			job.setJarByClass(topN.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			// job.setPartitionerClass(twopartitions.class);
			// job.setOutputFormatClass(fakeOutPutFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}
}
