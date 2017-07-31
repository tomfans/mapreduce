package com.isesol.mapreduce;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.PrintCommandListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.client.HTable;

public class binFileRead_forscala2 {

	public static void main (String[] args) {

		try {

			Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			FSDataInputStream inputStream = fs.open(new Path("d:\\b.gz"));
			InputStream in = codec.createInputStream(inputStream);
			// DataInputStream in = new DataInputStream(new
			// BufferedInputStream(new FileInputStream(fileName)));
			byte[] itemBuf = new byte[8];
			int i = 0;
			int j = 0;
			String result = "";
			while (true) {
				int readlength = in.read(itemBuf, 0, 8);
				if (readlength <= 0)
					break;
				double resultDouble = arr2double(itemBuf, 0);
				i++;
				j++;

				if (j == 1) {
					result = result + resultDouble;
				} else {
					result = result + "," + resultDouble;
				}

				if (i % 3 == 0) {
					System.out.println(result);
			
					result = "";
					j = 0;
				}

			}
			in.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static double arr2double(byte[] arr, int start) {
		int i = 0;
		int len = 8;
		int cnt = 0;
		byte[] tmp = new byte[len];
		for (i = start; i < (start + len); i++) {
			tmp[cnt] = arr[i];
			cnt++;
		}
		long accum = 0;
		i = 0;
		for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
			accum |= ((long) (tmp[i] & 0xff)) << shiftBy;
			i++;
		}
		return Double.longBitsToDouble(accum);
	}

	/*
	 * 通过APP ID获取要查询的ROWKEY，再查询字段
	 * 
	 * 
	 * 
	 * 
	 */
	public static ArrayList getHaseCols(String appId) throws ClassNotFoundException, IOException {
		ArrayList colList = new ArrayList<String>();
		Configuration hbaseconf = HBaseConfiguration.create();
		hbaseconf.set("hbase.zookeeper.quorum",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com");
		hbaseconf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseconf.set("maxSessionTimeout", "6");
		HTable table = new HTable(hbaseconf, "tab_col_config");
		Get get = new Get(appId.getBytes());
		Result result = table.get(get);
		for(int i = 0; i<= result.listCells().size() - 1; i++){
			colList.add(result.listCells().get(i).getQualifier());
		}
		
		return colList;
	}

}
