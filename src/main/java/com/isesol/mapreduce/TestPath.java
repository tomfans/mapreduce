package com.isesol.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class TestPath {

	public static void main(String[] args) {
		
		
		String fileName = "005be6f9-e311-4a3e-9143-b424d7a50e8d_B13612145_1524375169_1.txt";
		String machineNo = fileName.split("_")[1];
		String fileTimeStamp = fileName.split("_")[2];
		String fileNo = fileName.split("_")[3].split("\\.")[0];
		
		System.out.println(machineNo  + "  " + fileTimeStamp + "  "  + fileNo);

	}
	
}
