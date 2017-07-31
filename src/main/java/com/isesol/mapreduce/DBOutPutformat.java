package com.isesol.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DBOutPutformat {

	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		JobConf conf = new JobConf(configuration, DBOutPutformat.class);
		//conf.setInputFormat(DBInputFormat.class);
		Job job = Job.getInstance(conf, "DBOutPutformat");
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://10.215.4.161:3306/test","admin", "internal");
		
		job.setJarByClass(DBOutPutformat.class);

		job.setInputFormatClass(DBInputFormat.class);
		// Job job, Class inputClass, String tableName, String conditions,
		// String orderBy, String... fieldNames
		DBInputFormat.setInput(job, MyUser.class, "test", null, null, "id", "name");

		// 当map输出类型和reduce输出类型一致时，可以不设置
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 指定不需要使用reduce，直接把map任务的输出写入到HDFS中
		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		DistributedCache.addFileToClassPath(new Path("/user/hdfs/myjars/mysql-connector-java.jar"), configuration);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MyMapper extends Mapper<LongWritable, MyUser, Text, NullWritable> {

		protected void map(LongWritable key, MyUser value,
				Mapper<LongWritable, MyUser, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			context.write(new Text(line), NullWritable.get());
		}
	}

	public static class MyUser implements Writable, DBWritable {
		int id;
		String name;

		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.name = Text.readString(in);
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(id);
			Text.writeString(out, name);
		}

		public void readFields(ResultSet resultSet) throws SQLException {
			this.id = resultSet.getInt(1);
			this.name = resultSet.getString(2);
		}

		public void write(PreparedStatement statement) throws SQLException {
			statement.setInt(1, id);
			statement.setString(2, name);
		}

		@Override
		public String toString() {
			return this.id + ":" + this.name;
		}

	}

}
