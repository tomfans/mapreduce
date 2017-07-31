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
		try {
			Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			FSDataInputStream inputStream = fs.open(new Path(fileName));
			InputStream in = codec.createInputStream(inputStream);
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
					resultset.add(result);
					System.out.println(i + ":" + result);
					result = "";
					j = 0;
				}

			}
			// System.out.println(resultset.get(0));
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// FSDataOutputStream out = fs.create(new Path(outFileName));
	// IOUtils.copyBytes(in, out, conf);
	// IOUtils.closeStream(in);
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
}
