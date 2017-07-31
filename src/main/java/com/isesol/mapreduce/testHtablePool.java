package com.isesol.mapreduce;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class testHtablePool {

	public static void main(String[] args) throws IOException{
		
		Configuration configuration  = new Configuration();
		HConnection connection = HConnectionManager.createConnection(configuration);
		HTableInterface table = connection.getTable("test2");
		table.setAutoFlush(false);
		
		Configuration conf = HBaseConfiguration.create();
		HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
		
		HTableInterface table1 = pool.getTable("test2");
		
		table1.setAutoFlush(false);
		
		Connection conn = ConnectionFactory.createConnection(conf);
	
		Table table3 = conn.getTable(TableName.valueOf("test2"));
		
		table3.setWriteBufferSize(3*1024*1024);
		
		Table table4 = conn.getTable(TableName.valueOf("test2"));


		
		
		// use table as needed, the table returned is lightweight
		table1.close();
		// use the connection for other access to the cluster
		connection.close();
	}
}
