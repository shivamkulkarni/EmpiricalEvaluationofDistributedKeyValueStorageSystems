package com.mycompany.app;


import java.util.ArrayList;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;


//import java.lang.ProcessBuilder;
//import java.lang.Process;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongoDB_test{
    private static final String ip1 = "129.114.25.22";
    private static final String ip2 = "129.114.25.48";
    private static final String ip3 = "129.114.25.41";
    private static final String ip4 = "129.114.25.62";
    private static final String ip5 = "129.114.25.56";
    private static final String ip6 = "129.114.25.47";
    private static final String ip7 = "129.114.25.38";
    private static final String ip8 = "129.114.25.60";
    private static MongoClient mongo = null;
    private static DB db = null;
    private static DBCollection collection = null;
    private static String clientID = "";
    
    public static void insert(String key, String value){
        try{
            BasicDBObject tuple = new BasicDBObject();
            tuple.put("key", key);
            tuple.put("value", value);
            collection.insert(tuple);
            //System.out.println("----------INSERT----------");
        } catch (Exception e){
            System.out.println(e);
        }
    }

    public static String lookup(String key){
        String ret = "";
        try{
            BasicDBObject tuple = new BasicDBObject();
            tuple.put("key", key);
            DBCursor cursor = collection.find(tuple);
            while(cursor.hasNext()){
                ret = cursor.next().toString();
                return ret;
            }
            System.out.println(ret);
        }catch (Exception e){
            System.out.println(e);
        }
        return ret;
    }

    public static void remove(String key, String value){
        try{
            BasicDBObject tuple = new BasicDBObject();
            tuple.put("key", key);
            tuple.put("value", value);
            collection.remove(tuple);
            //System.out.println("----------REMOVE----------");
        } catch (Exception e){
            System.out.println(e);
        }
    }

    public static void main(String args[]){
        System.out.println("Enter ClientID to begin.");
        try{
            //BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            //clientID = reader.readLine();
            clientID = "client2";
            MongoCredential credential = MongoCredential.createCredential(clientID, "test_db", "group-8".toCharArray());
        //MongoCredential cred = MongoCredential.createCredential(clientID, "test_db", "group-8".toCharArray());
            mongo = new MongoClient(new ServerAddress(ip1, 27017), Arrays.asList(credential));

            db = mongo.getDB("test_db");
            collection = db.getCollection("cs550");

            /*
            BasicDBObject query = new BasicDBObject();
            query.put("key", "aaa");
            query.put("value", "bbb");
            collection.insert(query);
            System.out.println("Insert done");

            DBCursor cursor = collection.find(query);
            while(cursor.hasNext()){
                System.out.println("FIND " + cursor.next().toString());
            }
            */
            int i = 0;
            for(i = 0; i < 5; i++){
                String randKey = String.valueOf(Math.random());
                String randVal = String.valueOf(Math.random());
                insert(randKey, randVal);
                lookup(randKey);
                //remove(randKey, randVal);
            }
            System.out.println("Reach the end");
        } catch (Exception e){
            System.out.println(e);
            System.out.println("EXCEPTION!!");
        }
    }
}

//db.createUser({user:'client3',pwd:'group-8',roles: [{ role: 'readWrite', db:'test_db'}]})
