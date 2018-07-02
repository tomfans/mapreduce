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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.JSONObject;

public class binFileRead {

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		
		StringBuilder result = new StringBuilder();
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream("d:\\aaa.json")));
		String s = null;
		while ((s = in.readLine())!=null) {
			result.append(s);
		}
		
		ArrayList<String> col = getHaseCols(result.toString());
		
		for(int i=0; i< col.size(); i++){
			
			System.out.println(col.get(i));
			
		}
		

	}
		
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
}
