package com.mycompany.app;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.lang.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.Scanner;

public class Cassandra {

	private static Cluster cluster;
	private static Session session;
	
	public static void connect(String host) {
		cluster = Cluster.builder().addContactPoint(host).build();
		session = cluster.connect();
	}
	
	public static boolean insert(String key, String value) {
		// Insert one record into the table
		session.execute("USE test_for_con");
		String query = String.format("INSERT INTO Key_Values (key, value) VALUES ('%s', '%s')", key, value);
		session.execute(query);
		return true;
	}
	
	public static boolean remove(String key) {
		// Delete one record into the table
		String query = String.format("DELETE FROM Key_Values WHERE key = '%s'", key);
		session.execute(query);
		return true;
	}
	
	public static String lookup(String key) {
		// Delete one record into the table
		String query = String.format("SELECT value FROM Key_Values WHERE key = '%s'", key);
		ResultSet results = session.execute(query);
		for (Row row : results) {
			return row.getString("value");
		}
		return null;
	}
	
	public static void disconnect() {
		// Clean up the connection by closing it
		cluster.close();
	}
	
	private static String padString(String string, int length) {
		StringBuffer paddedString = new StringBuffer();
		paddedString.append(string);

		for (int i = 1; i <= length - (string.length()); i++) {
			paddedString.append("#");
		}
		return paddedString.toString();
	}
}
