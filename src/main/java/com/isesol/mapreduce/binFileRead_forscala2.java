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
import java.net.URL;
import java.security.cert.CollectionCertStoreParameters;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.net.PrintCommandListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapred.JobConf;
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

	public static void main(String[] args) throws IOException {

		unGzipFile("/tmp/03944c5e-8938-49f9-984e-edef5d81f711_20171031165756_0.gz");
/*
		try {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream inputStream = fs
					.open(new Path("c:\\03944c5e-8938-49f9-984e-edef5d81f711_20171031165756_0"));
			byte[] itemBuf = new byte[8];
			int i = 0;
			int j = 0;
			String result = "";
			while (true) {
				int readlength = inputStream.read(itemBuf, 0, 8);
				if (readlength <= 0)
					break;
				double resultDouble = arr2double(itemBuf, 0);

				// System.out.println(resultDouble);

				i++;
				j++;

				if (j == 1) {
					result = result + resultDouble;
				} else {
					result = result + "," + resultDouble;
				}

				if (i % 8 == 0) {
					System.out.println(result);

					result = "";
					j = 0;
				}

			}

			inputStream.close();

		} catch (Exception e) {
			e.printStackTrace();
		} */

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
		for (int i = 0; i <= result.listCells().size() - 1; i++) {
			colList.add(result.listCells().get(i).getQualifier());
		}

		return colList;
	}

	public static void unGzipFile(String sourcedir) {
		String ouputfile = "";
		try {
			// 建立gzip压缩文件输入流
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream inputStream = fs.open(new Path(sourcedir));
			//FileInputStream fin = new FileInputStream(sourcedir);
			// 建立gzip解压工作流
			GZIPInputStream gzin = new GZIPInputStream(inputStream);
			// 建立解压文件输出流
			ouputfile = sourcedir.substring(0, sourcedir.lastIndexOf('.'));
			//FileOutputStream fout = new FileOutputStream(ouputfile);

			FSDataOutputStream fout = fs.create(new Path( ouputfile));
			int num;
			byte[] buf = new byte[1024];

			while ((num = gzin.read(buf, 0, buf.length)) > 0) {
				fout.write(buf, 0, num);
			}

			gzin.close();
			fout.close();
		} catch (Exception ex) {
			System.out.println("there are some errors");
			System.err.println(ex.toString());
		}
		return;
	}

}
