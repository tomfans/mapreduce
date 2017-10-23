package com.isesol.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.io.netty.handler.codec.http.HttpContentEncoder.Result;
import com.sun.org.apache.bcel.internal.generic.NEW;


public class movingAverage {

	public static class TokenizerMapper extends Mapper<Object, Text, compositekey, Text> {

		private Text data = new Text();
		private compositekey newkey = new compositekey();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split(",");
			String stock = val[0];
			String time = val[1];
			String price = val[2];
			newkey.setStock(stock);
			newkey.setTime(val[1]);

			context.write(newkey, new Text(price));
		}

	}

	public static class twopartitions extends Partitioner<compositekey, Text> implements Configurable {

		public int getPartition(compositekey key, Text value, int numPartitions) {
			// TODO Auto-generated method stub

			return (key.getStock().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

		public void setConf(Configuration conf) {
			// TODO Auto-generated method stub
		}

		public Configuration getConf() {
			// TODO Auto-generated method stub
			return null;
		}

	}

	public static class compositekeyComparator extends WritableComparator {

		public compositekeyComparator() {
			super(compositekey.class, true);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			compositekey a1 = (compositekey) a;
			compositekey b1 = (compositekey) b;

			int compare = a1.getStock().compareTo(b1.getStock());
			if (compare != 0) {
				return compare;
			} else {
				return a1.getTime().compareTo(b1.getTime());
			}

		}

	}

	public static class compositekey implements WritableComparable<compositekey> {

		private String stock;
		private String time;

		public void setStock(String stock) {

			this.stock = stock;
		}

		public String getStock() {

			return this.stock;
		}

		public void setTime(String time) {
			this.time = time;
		}

		public String getTime() {
			return time;
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(this.getStock());
			out.writeUTF(this.getTime());
		}

		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub

			stock = in.readUTF();
			time = in.readUTF();
		}

		public String toString() {
			return stock + "," + time;
		}

		public int compareTo(compositekey o) {
			// TODO Auto-generated method stub
			return 0;
		}

	}

	public static class DefinedGroupSort extends WritableComparator {

		protected DefinedGroupSort() {
			super(compositekey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			compositekey a1 = (compositekey) a;
			compositekey b1 = (compositekey) b;

			return a1.getStock().compareTo(b1.getStock());
		}

	}

	
	public static class IntSumReducer extends Reducer<compositekey, Text, Text, DoubleWritable> {

		//private int windowsize = 3;
		private double result = 0.0;
		
		public void reduce(compositekey key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
					
			simplemovingaverage smg = new simplemovingaverage();
			
			for (Text values : value) {
				
				//每个value添加到Queue，然后计算移动平均数，直接就返回
				
				smg.addNewNumber(Integer.parseInt(values.toString()));

				result = smg.getMovingAverage();
				context.write(new Text(key.getStock()), new DoubleWritable(result));
			}

		//	context.write(new Text(key.getStock()), new DoubleWritable(result));

		}
	}
	
	
	//实现移动平均的算法，通过Queue来实现
	
	public static class simplemovingaverage {
		
		private double sum = 0.0;
		private int period = 3;
		private final Queue<Double> window = new LinkedList<Double>();
		
		public void addNewNumber(double number) {
			sum += number;
			window.add(number);
			if(window.size() > period) {
				sum -=window.remove();
			}
		}
		
		public double getMovingAverage(){
			if(window.isEmpty()){
				throw new IllegalArgumentException("undefined");
			}
			return sum / window.size();
		}
		
	}

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "movingAverage");
		job.setJarByClass(movingAverage.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(compositekey.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setPartitionerClass(twopartitions.class);
		job.setSortComparatorClass(compositekeyComparator.class);
		job.setGroupingComparatorClass(DefinedGroupSort.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
