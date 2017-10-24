package com.isesol.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class mapjoin {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text data = new Text();
		private IntWritable result = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] val = value.toString().split("\\s+");
			if (val.length == 2) {
				String id = val[0];
				String name = val[1];
				context.write(new Text(id), new Text("a#" + name));
			} else if (val.length == 3) {
				String id = val[0];
				String statyear = val[1];
				String num = val[2];
				context.write(new Text(id), new Text("b#" + statyear + "#" + num));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			Vector<String> vecA = new Vector<String>();
			Vector<String> vecB = new Vector<String>();
			System.out.println("new line is " + value.toString());
			for (Text values : value) {
				String val = values.toString();
				if (val.startsWith("a#")) {
					vecA.add(val.substring(2));
				} else if (val.startsWith("b#")) {
					vecB.add(val.substring(2));
				}
			}

			int sizeA = vecA.size();
			int sizeB = vecB.size();

			System.out.println("key is " + key + "sizeA is " + sizeA + " sizeB is " + sizeB);

			int i, j;
			for (i = 0; i < sizeA; i++) {
				for (j = 0; j < sizeB; j++) {
					context.write(key, new Text(vecA.get(i) + "#" + vecB.get(j)));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "mapjoin");
		job.setJarByClass(mapjoin.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


