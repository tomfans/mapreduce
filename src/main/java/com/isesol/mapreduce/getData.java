package com.isesol.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import com.sun.glass.ui.Size;
import com.sun.jndi.url.iiopname.iiopnameURLContextFactory;

public class getData implements Serializable {
	public static void main(String[] args) {
		
		List<String> list = new ArrayList<String>();
		list.add("a");
		list.add("b");
		list.add("c");
		list.add("d");
		
		List<List<String>> list1 = new ArrayList<List<String>>();
		
		list1 = findSortMBA.findsort(list, 2);
		
		for(List<String> alist : list1){
			System.out.println(alist.get(0) + "-" + alist.get(1));
		}
		
	}
}
