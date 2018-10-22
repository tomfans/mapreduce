package com.isesol.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class autoAdapt_java {

	public static void main(String[] args) throws IOException, InterruptedException {

		String fileName = args[0];
		String appId = args[1];
		String machine_tool = args[2];
		String bizId = args[3];
		String bizData = args[4]; // no use in 非i5自适应
		String collectType = args[5];
		String table = ""; // default collect type is None
		List<Put> list = new ArrayList<Put>();

		System.out.println("fileName is " + fileName);
		System.out.println("bizId is " + bizId);
		System.out.println("machine_tool is " + machine_tool);
		System.out.println("collectType is " + collectType);

		if (collectType.equals("tooladapt") || collectType.equals("tooladaptSingle")) {
			  table = "t_high_frequently_tooladapt";
			 // table = "test";
		} else {
			throw new IllegalStateException("No table name found");
		}

		System.out.println("table name is " + table);

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"datanode01-ucloud.isesol.com,datanode02-ucloud.isesol.com,datanode03-ucloud.isesol.com,namenode01-ucloud.isesol.com,namenode02-ucloud.isesol.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		HTable myTable = new HTable(conf, TableName.valueOf(table));
		readFileByLines(fileName, list, machine_tool, collectType);

		System.out.println("read file by line function finished,start to put data into HBase");
		
		myTable.put(list);
		myTable.flushCommits();
		myTable.close();
		System.out.println("job has been finished!");
	}

	public static void readFileByLines(String filePath, List<Put> list, String machine_tool, String collectType)
			throws IOException {

		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.addResource("conf/core-site.xml");
		conf.addResource("conf/hdfs-site.xml");
		FileSystem fs = FileSystem.get(conf);
		String tempString = null;
		int ran = 1000000;
		Put put = null;

		String fileName = filePath.split("/")[filePath.split("/").length - 1];
		String timestamp = fileName.split("_")[1];
		String fileNum = fileName.split("_")[2].split("\\.")[0];
		FSDataInputStream inputStream = fs.open(new Path(filePath));
		fileNum = String.valueOf(Integer.parseInt(fileNum) + 1000);

		try {
			
			while ((tempString = inputStream.readLine()) != null) {
				if (tempString != null) {
					String[] cols = tempString.split("\\|\\|");
					String rowkey = String.valueOf(Calendar.getInstance().getTimeInMillis());
					put = new Put(
							Bytes.toBytes(machine_tool + "#" + timestamp + "#" + fileNum + "#" + rowkey + "#" + ran));
					ran = ran + 1;
					for (int i = 0; i < cols.length; i++) {
						String colVal = cols[i].split(":").length > 1?cols[i].split(":")[1]:"0";
						String colName = cols[i].split(":")[0];
						put.add(Bytes.toBytes("cf"), Bytes.toBytes(colName), Bytes.toBytes(colVal));
						if (colName.equalsIgnoreCase("ext_toolno")) {
							System.out.println("start to put T");
							put.add(Bytes.toBytes("cf"), Bytes.toBytes("T"), Bytes.toBytes(colVal));
						}
						if (colName.equalsIgnoreCase("cnc_rdspmeter[0]")) {
							put.add(Bytes.toBytes("cf"), Bytes.toBytes("SPDMTLOAD"), Bytes.toBytes(colVal));
						}

					}
					put.add(Bytes.toBytes("cf"), Bytes.toBytes("collect_type"), Bytes.toBytes(collectType));
					list.add(put);
				}
			}
			inputStream.close();
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("erros happended on get HDFS file");
		} finally {
			if (inputStream != null && fs != null) {
				try {
					inputStream.close();
					fs.close();
				} catch (IOException e1) {
				}
			}
		}
	}

}
