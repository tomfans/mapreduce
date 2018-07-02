package com.isesol.mapreduce;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class unCompressHDFSFile {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		ArrayList resultset = new ArrayList();
		String fileName = args[0];
		String tempString;
		try {
		//	Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
			Configuration conf = new Configuration();
			conf.addResource("conf/core-site.xml");
			conf.addResource("conf/hdfs-site.xml");
			FileSystem fs = FileSystem.get(conf);
		//	CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			FSDataInputStream inputStream = fs.open(new Path(fileName));
			//InputStream in = inputStream.createInputStream(inputStream);
			while ((tempString = inputStream.readLine()) != null) {
				
				System.out.println(tempString);
			
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
