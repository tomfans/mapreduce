package com.isesol.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;


public class filterTest {
	public static void main(String[] args) throws IOException {
		
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Configuration hbaseconf = HBaseConfiguration.create();
		hbaseconf.set("hbase.zookeeper.quorum",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com");
		hbaseconf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseconf.set("user", "hdfs");
		HTable htable = new HTable(hbaseconf, "usertable");
		HTable htable1 = new HTable(hbaseconf, "test2");
		Scan scan1 = new Scan();
		scan1.setCaching(300);
		
		Filter rowfilter = new RowFilter(CompareOp.EQUAL,
				new BinaryPrefixComparator(Bytes.toBytes("user1019314459807976796")));
		Filter rowfilter1 = new RowFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("A131420033-1007-9223370539574828268"))); 

		// scan1.setRowPrefixFilter(Bytes.toBytes("A131420033-1007-9223370539574828268"));
		// Filter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"),
		// Bytes.toBytes("fault_level2_name"), CompareOp.EQUAL,
		// Bytes.toBytes("电气问题"));
		// scan1.setFilter(rowfilter);
		scan1.setRowPrefixFilter(Bytes.toBytes("user1019314459807976796"));
		ResultScanner scaner = htable.getScanner(scan1);
		List<Put> list = new ArrayList<Put>();
		Result result = null;
		
		htable1.setWriteBufferSize(6*1024*1024);
		//htable1.setAutoFlush(false);
		
		while (scaner.iterator().hasNext()) {

			result = scaner.next();
			Put put = new Put(result.getRow());
			//put.setWriteToWAL(false);
			for (int i = 0; i <= result.listCells().size() - 1; i++) {
				put.add("cf".getBytes(), Bytes.toBytes(new String(result.listCells().get(i).getQualifier())), result
						.getValue("cf".getBytes(), new String(result.listCells().get(i).getQualifier()).getBytes()));
			}
		
		}
		
		htable1.put(list);
		htable1.close();
		htable.close();
		
		System.out.println("Job finish" + dateformat.format(System.currentTimeMillis()));
	}

}
