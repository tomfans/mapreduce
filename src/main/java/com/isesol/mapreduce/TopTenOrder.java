package com.isesol.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.net.nntp.NewsgroupInfo;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class TopTenOrder {

	public static String driver = "com.mysql.jdbc.Driver";
	public static String url = "jdbc:mysql://10.215.4.161:3306/test";
	public static String user = "admin";
	public static String password = "internal";
	
	public static Connection getconn() throws ClassNotFoundException, SQLException{
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url, user, password);
		conn.setAutoCommit(false);
		//return conn.createStatement();
		System.out.println("start to connect database, and return");
		return conn;
	}
	
	public static class TokenizerMapper extends Mapper<Object, Text, NullWritable, IntWritable> {

		private TreeSet<Integer> top10 = new TreeSet<Integer>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			top10.add(Integer.parseInt(value.toString()));
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			while (top10.size() > 10) {
				top10.remove(top10.first());
			}

			Iterator iterator = top10.iterator();
			while (iterator.hasNext()) {
				context.write(NullWritable.get(), new IntWritable(Integer.parseInt(iterator.next().toString())));
			}
		}
	}

	public static class twopartitions extends Partitioner<NullWritable, IntWritable> implements Configurable {

		@Override
		public int getPartition(NullWritable key, IntWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			return value.get() % 2;
		}

		public void setConf(Configuration conf) {
			// TODO Auto-generated method stub

		}

		public Configuration getConf() {
			// TODO Auto-generated method stub
			return null;
		}

	}

	public static class fakeOutPutFormat extends OutputFormat<NullWritable, IntWritable> {

		@Override
		public RecordWriter<NullWritable, IntWritable> getRecordWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return new fakeRecordWrite();
		}

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return (new org.apache.hadoop.mapreduce.lib.output.NullOutputFormat<NullWritable, IntWritable>())
					.getOutputCommitter(context);

		}

	}

	public static class fakeRecordWrite extends RecordWriter<NullWritable, IntWritable> {

		@Override
		public void write(NullWritable key, IntWritable value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			try {
				String sql = "insert into test values (" + Integer.parseInt(value.toString()) + ")";
				System.out.println(sql);
				Statement statement = getconn().createStatement();
				
			    statement.execute(sql);
				statement.close();
				//conn.close();
				System.out.println("insert is successfuly " + value.toString());

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

	}

	public static class IntSumReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : value) {
				context.write(NullWritable.get(), val);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopTenOrder");
		job.setJarByClass(TopTenOrder.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2);
		job.setPartitionerClass(twopartitions.class);
		job.setOutputFormatClass(fakeOutPutFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
