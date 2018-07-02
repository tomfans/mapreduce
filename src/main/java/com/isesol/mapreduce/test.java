package com.isesol.mapreduce;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.RandomStringUtils;

public class test {
	
	public static void main(String[] args){
		String[] str = null;
		String fileName="/tmp/0082a104-2ecc-4567-a374-838fa016d393_1524033252_1.txt";
		
		String aString = fileName.split("/")[fileName.split("/").length - 1];
		String timestamp = aString.split("_")[1];
		
		System.out.println(timestamp);
		
	}
	
}
