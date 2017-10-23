package com.isesol.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;
import org.apache.avro.reflect.DateAsLongEncoding;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.nntp.NewsgroupInfo;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue.RawBytesComparator;
import org.apache.hadoop.hbase.quotas.OperationQuota.AvgOperationSize;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ExampleCompositeKey {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split(",");
			String year = val[0];
			String wendu = val[3];
			context.write(new Text(year), new Text(wendu));
		}
	}
	
	
	public static class IntSumReducer extends Reducer<Text, Text, Text, secondary_key> {

		private secondary_key newkey = new secondary_key();
		private int count = 0;
		private int i = 0;
		
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {

			for (Text values : value) {
				
				count += Integer.parseInt(values.toString());
				i++;
			}

			int avg = count / i ;
			newkey.setAvg(avg);
			newkey.setCount(count);

			
			context.write(key, newkey);

		}
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ExampleCompositeKey");
		job.setJarByClass(ExampleCompositeKey.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(secondary_key.class);
		//job.setPartitionerClass(twopartitions.class);
		//job.setSortComparatorClass(compositekeyComparator.class);
		//job.setGroupingComparatorClass(DefinedGroupSort.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
