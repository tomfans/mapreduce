package com.isesol.mapreduce;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class HbaseLinesCount {

	public static class TokenizerMapper_hbase extends TableMapper<Text, Text> {
		public void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {

			List<KeyValue> kvs = value.list();
			for (KeyValue val : kvs) {
				String colname = new String(val.getQualifier());
				String colvalue = new String(val.getValue());
				//System.out.println("column name is " + colname + " column values is " + colvalue);
				if(colvalue.equals("东莞天驰电子科技有限公司")) {
					
					System.out.println("the key is "  + val.getRow().toString() + " column values is " + colvalue);
					context.write(new Text(val.getRow()), new Text(colname + "#" + colvalue));
				}
				
			}
		}
	}

	public static class IntSumReducer_hbase extends TableReducer<Text, Text, ImmutableBytesWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Put put = new Put(Bytes.toBytes(key.toString()));
			for (Text i : values) {

				String val[] = i.toString().split("#");
				if (val.length == 2) {
					String colname = val[0];
					String colvalue = val[1];
					put.add(Bytes.toBytes("cf"), Bytes.toBytes(colname), Bytes.toBytes(colvalue));
				} 
			}

			context.write(null, put);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Scan scan = new Scan();
		//scan.addFamily(Bytes.toBytes("cf"));
	    scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("CLIENT_NAME"));
		// scan.setMaxVersions();
		Job job = new Job(conf, "t_a_statistic");
		job.setJarByClass(HbaseLinesCount.class);
		// job.setMapperClass(TokenizerMapper_hbase.class);
		// job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		// job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableMapperJob("t_a_statistic", scan, TokenizerMapper_hbase.class, Text.class, Text.class,
				job);
		TableMapReduceUtil.initTableReducerJob("test11", IntSumReducer_hbase.class, job);
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
