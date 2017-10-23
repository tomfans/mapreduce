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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.COL;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class secondary_sort {

	public static class TokenizerMapper extends Mapper<Object, Text, compositekey, Text> {

		private Text data = new Text();
		private compositekey newkey = new compositekey();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split(",");
			String year = val[0];
			String month = val[1];
			newkey.setYear(year);
			newkey.setWendu(val[3]);

			System.out.println(newkey.getYear() + "-" + newkey.getWendu());

			context.write(newkey, new Text(val[3]));
		}
	}

	public static class IntSumReducer extends Reducer<compositekey, Text, Text, Text> {

		public void reduce(compositekey key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {

			StringBuilder str = new StringBuilder("");

			for (Text values : value) {

				String val = values.toString();
				str.append(val);
				str.append(",");
			}

			context.write(new Text(key.getYear()), new Text(str.toString()));

		}
	}

	public static class compositekeyComparator extends WritableComparator {

		public compositekeyComparator() {
			super(compositekey.class, true);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			compositekey a1 = (compositekey) a;
			compositekey b1 = (compositekey) b;

			int compare = Integer.parseInt(a1.getYear()) - Integer.parseInt(b1.getYear());
			if (compare != 0) {
				return -1 * compare;
			} else {
				return Integer.parseInt(a1.getWendu()) - Integer.parseInt(b1.getWendu());
			}

		}

	}

	public static class compositekey implements WritableComparable<compositekey> {

		private String year;
		private String wendu;

		public void setYear(String year) {

			this.year = year;
		}

		public String getYear() {

			return this.year;
		}

		public void setWendu(String wendu) {
			this.wendu = wendu;
		}

		public String getWendu() {
			return wendu;
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(this.getYear());
			out.writeUTF(this.getWendu());
		}

		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub

			year = in.readUTF();
			wendu = in.readUTF();
		}

		public String toString() {
			return year + "," + wendu;
		}

		public int compareTo(compositekey o) {
			// TODO Auto-generated method stub
			return 0;
		}

	}

	public static class twopartitions extends Partitioner<compositekey, Text> implements Configurable {

		public int getPartition(compositekey key, Text value, int numPartitions) {
			// TODO Auto-generated method stub

			System.out.println("aa-" + key.getYear() + "-" + key.getWendu());
			return (key.getYear().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

		public void setConf(Configuration conf) {
			// TODO Auto-generated method stub
		}

		public Configuration getConf() {
			// TODO Auto-generated method stub
			return null;
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

			return Integer.parseInt(a1.getYear()) - Integer.parseInt(b1.getYear());
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "secondary_sort");
		job.setJarByClass(secondary_sort.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(compositekey.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(twopartitions.class);
		job.setSortComparatorClass(compositekeyComparator.class);
		job.setGroupingComparatorClass(DefinedGroupSort.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
