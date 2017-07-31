package com.isesol.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class TestPath {

	public static void main(String[] args) {
		String fileName = "b";
		Path path  = Paths.get("D:/", fileName);
		try {
			FileInputStream inputStream = new FileInputStream(path.toFile());
			byte[] inputBuffer = new byte[8];
			inputStream.read(inputBuffer, 0, 8);
			double resultDouble = arr2double(inputBuffer,0);
	        System.out.println(resultDouble);

			inputStream.close();
	    } catch (IOException e1) {
	    	
	        e1.printStackTrace();
	    }
	}
	
	public static double arr2double (byte[] arr, int start) {
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
		for ( int shiftBy = 0; shiftBy < 64; shiftBy += 8 ) {
			accum |= ( (long)( tmp[i] & 0xff ) ) << shiftBy;
			i++;
		}
		return Double.longBitsToDouble(accum);
	}

}
