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

		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://10.215.4.161:3306/test";
		String user = "admin";
		String password = "internal";
		try {
			System.out.println("it already gets into insert function");
			System.out.println("1");
			Class.forName(driver);
			System.out.println("2");
			Connection conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(true);
			Statement statement = conn.createStatement();
			String sql = "insert into test (id) values (" + 1 + ")";
			System.out.println(sql);
			statement.execute(sql);
			statement.close();
			conn.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
