package com.mycompany.app;

import java.io.BufferedReader;
import java.io.File;
//import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.lang.*;
import java.io.BufferedReader;
import java.io.FileReader;


import java.util.*;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

public class Exp_runner {
    public static final int NUM_EXP = 1000000;
    public static final int KEY_BYTES = 10;
    public static final int VALUE_BYTES = 90;
    // final String alphabet =
    // "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static ArrayList<byte[]> keys = new ArrayList<>();
    public static ArrayList<byte[]> values = new ArrayList<>();
    public static Integer NUM_NODES = 1;
    public static String myIP = "";
    public static String myCID = "";
    
    
    
    public static void main(String[] args) throws IOException {
    	runCassandraExp();
    	runRedisExp();
    	runMongoExp();   
    }

	public static void runCassandraExp()
	{
		Scanner scan = new Scanner(System.in);
		Cassandra.connect("192.168.8.12");
		System.out.println("Connection established enter clients name");
		
		String client = scan.nextLine();
		long startTimeInsert = System.currentTimeMillis();
		for (int i = 1; i <= 100000; i++) {
			String toPad = client + Long.toString(i);
			String key = padString(toPad, 10);
			String value = padString(toPad, 90);
			
			insert(key,value);
			//System.out.println("Insert Done for key"+i);
		}
		long endTimeInsert = System.currentTimeMillis();
		
		double timeTakenForInsert = (endTimeInsert - startTimeInsert)/1000;
		System.out.println("Time taken for insert in seconds was "+ timeTakenForInsert);
		
		
		long startTimeLookup = System.currentTimeMillis();
		for (int i = 1; i <= 100000; i++) {
			String toPad = client + Long.toString(i);
			String key = padString(toPad, 10);
			String val = Cassandra.lookup(key);
			//System.out.println("We Found "+val);
		}
		long endTimeLookup = System.currentTimeMillis();
		
		double timeTakenForLookup = (endTimeLookup - startTimeLookup)/1000;
		System.out.println("Time taken for lookup in seconds was "+ timeTakenForLookup);
		
		
		long startTimeRemove = System.currentTimeMillis();
		for (int i = 1; i <= 100000; i++) {
			String toPad = client + Long.toString(i);
			String key = padString(toPad, 10);
			Boolean res = Cassandra.remove(key);
			/*if(res)
			{
				System.out.println("We Deleted key"+i);
			}*/
		}
		long endTimeRemove = System.currentTimeMillis();
		
		double timeTakenForRemove = (endTimeRemove - startTimeRemove)/1000;
		System.out.println("Time taken for remove in seconds was "+ timeTakenForRemove);
		
		double totalTimeTaken = timeTakenForInsert + timeTakenForLookup + timeTakenForRemove;
		double latency = totalTimeTaken*1000/300000;
		double throughput = 300000/totalTimeTaken;
		System.out.println("Troughput was  "+throughput);
		System.out.println("Latency was  "+latency);
		
		disconnect();

	}
	
	public static void runRedisExp()
	{
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		
		jedisClusterNodes.add(new HostAndPort("192.168.8.12", 7003));
		jedisClusterNodes.add(new HostAndPort("192.168.8.11", 7003));
		jedisClusterNodes.add(new HostAndPort("192.168.8.23", 7003));
		jedisClusterNodes.add(new HostAndPort("192.168.8.16", 7003));
		jedisClusterNodes.add(new HostAndPort("192.168.8.13", 7003));
		jedisClusterNodes.add(new HostAndPort("192.168.8.36", 7003));
		jedisClusterNodes.add(new HostAndPort("192.168.8.17", 7003));
		Scanner scan = new Scanner(System.in);
		Redis.connect(jedisClusterNodes);
		
		System.out.println("Connection established enter clients name");
		
		String client = scan.nextLine();
		long startTimeInsert = System.currentTimeMillis();
		for (int i = 1; i <= 100000; i++) {
			String toPad = client + Long.toString(i);
			String key = padString(toPad, 10);
			String value = padString(toPad, 90);
			
			boolean suc = Redis.insert(key,value);
			//System.out.println("Insert Done for key"+i);
		}
		long endTimeInsert = System.currentTimeMillis();
		
		double timeTakenForInsert = (endTimeInsert - startTimeInsert)/1000;
		System.out.println("Time taken for insert in seconds was "+ timeTakenForInsert);
		
		
		long startTimeLookup = System.currentTimeMillis();
		for (int i = 1; i <= 100000; i++) {
			String toPad = client + Long.toString(i);
			String key = padString(toPad, 10);
			String val = Redis.lookup(key);
			//System.out.println("We Found "+val);
		}
		long endTimeLookup = System.currentTimeMillis();
		
		double timeTakenForLookup = (endTimeLookup - startTimeLookup)/1000;
		System.out.println("Time taken for lookup in seconds was "+ timeTakenForLookup);
		
		
		long startTimeRemove = System.currentTimeMillis();
		for (int i = 1; i <= 100000; i++) {
			String toPad = client + Long.toString(i);
			String key = padString(toPad, 10);
			Boolean res = Redis.remove(key);
			/*if(res)
			{
				System.out.println("We Deleted key"+i);
			}*/
		}
		long endTimeRemove = System.currentTimeMillis();
		
		double timeTakenForRemove = (endTimeRemove - startTimeRemove)/1000;
		System.out.println("Time taken for remove in seconds was "+ timeTakenForRemove);
		
		double totalTimeTaken = timeTakenForInsert + timeTakenForLookup + timeTakenForRemove;
		double latency = totalTimeTaken*1000/300000;
		double throughput = 300000/totalTimeTaken;
		System.out.println("Troughput was  "+throughput);
		System.out.println("Latency was  "+latency);
		
				
		//disconnect();
		//jc.close();
	}
	}
	
	public static void runMongoExp()
	{
		File f = new File("/home/cc/output.txt");
        if(f.createNewFile()){
            PrintStream fout = new PrintStream("/home/cc/output.txt");
            System.setOut(fout);
            System.out.println("Start time " + System.currentTimeMillis());
        }
        //if(args.length == 1) NUM_NODES = Integer.parseInt(args[1]);
        HashMap<String, String> server2IP = new HashMap<String, String>();
        server2IP.put("ip1", "129.114.25.22");
        server2IP.put("ip2", "129.114.25.48");
        server2IP.put("ip3", "129.114.25.41");
        server2IP.put("ip4", "129.114.25.62");
        server2IP.put("ip5", "129.114.25.56");
        server2IP.put("ip6", "129.114.25.47");
        server2IP.put("ip7", "129.114.25.38");
        server2IP.put("ip8", "129.114.25.60");

        System.out.println("Press Enter to start experiment");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            reader.readLine();

            URL url_name = new URL("http://bot.whatismyipaddress.com");
            reader = new BufferedReader(new InputStreamReader(url_name.openStream()));
            myIP = reader.readLine().trim();

            for(int i = 0; i < NUM_NODES; i++){
                if(myIP.equals(server2IP.get("ip"+ (i + 1)))){
                    myCID = "client" + (i + 1);
                }
            }
            System.out.println(myIP + " " + myCID);
            // Enable MongoDB logging in general
            System.setProperty("DEBUG.MONGO", "true");
            // Enable DB operation tracing
            System.setProperty("DB.TRACE", "true");

            //MongoDB_test mdb = new MongoDB_test(myIP, myCID);
            //mdb.connect(myIP, myCID);
            MongoDB_test mdb = new MongoDB_test("129.114.25.22", "client1");
            mdb.connect("129.114.25.22", "client1");
            //Insert
            long mongo_time = 0;
            long cas_time = 0;
            long red_time = 0;
            for(int i = 0; i < NUM_EXP; i++){
                byte[] key = new byte[10];
                byte[] value = new byte[90];
                new Random().nextBytes(key);
                keys.add(key);
                new Random().nextBytes(value);
                values.add(value);
                long t1 = System.currentTimeMillis();
                mdb.insert(key.toString(), value.toString());
                long t2 = System.currentTimeMillis();
                mongo_time += t2 - t1;
            }
            System.out.println("MongoDB insert throughput = " + NUM_EXP / (double)mongo_time);
            
            System.out.println("MongoDB insert latency = " + ((double)mongo_time) / NUM_EXP);
            
            //lookup
            mongo_time = 0;
            for(int i = 0; i < NUM_EXP; i++){
                byte[] key = keys.get(i);
                long t1 = System.currentTimeMillis();
                mdb.lookup(key.toString());
                long t2 = System.currentTimeMillis();
                mongo_time += t2 - t1;
            }
            System.out.println("MongoDB lookup throughput " + NUM_EXP / (double)mongo_time);
            
            System.out.println("MongoDB lookup latency = " + ((double)mongo_time) / NUM_EXP);
            
            //remove
            mongo_time = 0;
            for(int i = 0; i < NUM_EXP; i++){
                byte[] key = keys.get(i);
                byte[] value = values.get(i);
                long t1 = System.currentTimeMillis();
                mdb.remove(key.toString(), value.toString());
                long t2 = System.currentTimeMillis();
                mongo_time += t2 - t1;
            }
            System.out.println("MongoDB remove throughput " + NUM_EXP / (double)mongo_time);
            System.out.println("MongoDB remove latency = " + ((double)mongo_time) / NUM_EXP);
            System.out.println("End at " + System.currentTimeMillis());
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
}

