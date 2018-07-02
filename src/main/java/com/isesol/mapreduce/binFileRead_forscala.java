package com.isesol.mapreduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

import com.sun.imageio.spi.RAFImageInputStreamSpi;

public class binFileRead_forscala {

	public static ArrayList<String> binFileOut(String fileName, int cols) throws ClassNotFoundException {
		ArrayList resultset = new ArrayList();

		try {

			// 解压缩gz文件，并写入新的文件
			unGzipFile(fileName);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream inputStream = fs.open(new Path(fileName.substring(0, fileName.lastIndexOf('.'))));
			byte[] itemBuf = new byte[8];
			int i = 0;
			int j = 0;
			String result = "";
			while (true) {
				int readlength = inputStream.read(itemBuf, 0, 8);
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

				if (i % cols == 0) {
					resultset.add(result);
					result = "";
					j = 0;
				}

			}
			inputStream.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		cleanFile(fileName);
		return resultset;
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
	public static ArrayList<String> getHaseCols(String bizData) throws ClassNotFoundException, IOException {
		ArrayList colList = new ArrayList<String>();
		int i;
		JSONObject obj = new JSONObject(bizData);
		JSONObject obj2 = new JSONObject(obj.getString("ipx_bigData_cmd-isesol"));
		String param = obj2.getString("collectParam");
		String val[] = param.split("\\|");
		for (i = 0; i < val.length; i++) {
			colList.add(val[i]);
		}
		return colList;
	}

	// 解压缩 gzip文件，然后生成新的非GIZP文件。

	public static void unGzipFile(String sourcedir) {
		String ouputfile = "";
		try {
			// 建立gzip压缩文件输入流
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			if(! fs.exists(new Path(sourcedir))){
				System.out.println("the file is not exists");
			}
			FSDataInputStream inputStream = fs.open(new Path(sourcedir));
			// 建立gzip解压工作流
			GZIPInputStream gzin = new GZIPInputStream(inputStream);
			// 建立解压文件输出流
			ouputfile = sourcedir.substring(0, sourcedir.lastIndexOf('.'));

			FSDataOutputStream fout = fs.create(new Path(ouputfile));

			int num;
			byte[] buf = new byte[1024];

			while ((num = gzin.read(buf, 0, buf.length)) > 0) {
				fout.write(buf, 0, num);
			}

			System.out.println("ungzip is successful,the file name is " + ouputfile);
			gzin.close();
			fout.close();
		} catch (Exception ex) {
			System.out.println("ungip process errors");
			System.err.println(ex.toString());
		}
		return;
	}

	
	public static void cleanFile(String filename) {

		String ouputfile = "";
		try {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			ouputfile = filename.substring(0, filename.lastIndexOf('.'));
			fs.delete(new Path(ouputfile));
			System.out.println("clean file successful!");

		} catch (Exception ex) {
			System.out.println("clean file failed!");
			System.err.println(ex.toString());
		}
	} 

}
