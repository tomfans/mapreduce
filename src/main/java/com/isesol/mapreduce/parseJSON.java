package com.isesol.mapreduce;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONArray;

public class parseJSON {

	public static void main(String[] args) throws JSONException, IOException {
		StringBuilder result = new StringBuilder();
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream("d:\\aaa.json")));
		String s = null;
		while ((s = in.readLine())!=null) {
			result.append(s);
		}
		ArrayList colList = new ArrayList<String>();
		int i;
		JSONObject obj = new JSONObject(result.toString());  
		JSONObject obj2 = new JSONObject( obj.getString("ipx_bigData_cmd-isesol"));
		System.out.println(obj2.getString("collectParam"));
		String param = obj2.getString("collectParam");
		String val[] = param.split("\\|");
		for(i = 0; i < val.length; i++){
			colList.add(val[i]);
		}

		System.out.println(colList.size());
	}

}
