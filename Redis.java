package com.mycompany.app;

import java.util.*;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import java.util.Scanner;

public class Redis {

	private static Jedis redis = null;
	private static JedisCluster redisCluster = null;
	
	public static void connect(String host, int port) {
		try {
			redis = new Jedis(host, port);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Redis Connection Successful.");
	}
	
	public static void connect(Set<HostAndPort> clusterNodes) {
		try {
			redisCluster = new JedisCluster(clusterNodes);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Redis Connection Successful.");
	}

	public static boolean insert(String key, String value) {
		if (redis != null) {
			redis.set(key, value);
		} else {
			redisCluster.set(key, value);
		}
		return true;
	}

	public static boolean remove(String key) {
		if (redis != null) {
			redis.del(key);
		} else {
			redisCluster.del(key);
		}
		return true;
	}

	public static String lookup(String key) {
		if (redis != null) {
			return redis.get(key);
		} else {
			return redisCluster.get(key);
		}
	}
	
	/*public static void disconnect() {
		if (redis != null) {
			redis.close();
		} else {
			redisCluster.close();
		}
	}*/
	
	private static String padString(String string, int length) {
		StringBuffer paddedString = new StringBuffer();
		paddedString.append(string);

		for (int i = 1; i <= length - (string.length()); i++) {
			paddedString.append("#");
		}
		return paddedString.toString();
	}

}
