package com.isesol.mapreduce;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.visibility.VisibilityExpEvaluator;
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

import com.sun.jndi.url.iiopname.iiopnameURLContextFactory;

import jdk.internal.org.objectweb.asm.tree.analysis.Value;

public class testHtablePool {

	public static void main(String[] args) throws IOException{
		
		Configuration conf  = new Configuration();
        conf.set("hbase.zookeeper.quorum",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable("test2");
		table.setAutoFlush(false);
	
		
		Table table4 = connection.getTable(TableName.valueOf("test2"));
		Scan scan = new Scan();
		ResultScanner resultScanner = table4.getScanner(scan);

		for(Result res : resultScanner) {
			System.out.println(res.getColumn(Bytes.toBytes("cf"), Bytes.toBytes("name")));
		}
		
		// use table as needed, the table returned is lightweight
		table4.close();
		// use the connection for other access to the cluster
		connection.close();
	}
}
