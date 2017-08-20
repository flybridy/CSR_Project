package com.fleety.db;

import server.db.DbServer;

public class SqlDbServer extends DbServer {
	private static SqlDbServer singleInstance = null;
	public static SqlDbServer getSingleInstance(){
		if(singleInstance == null){
			synchronized(DbServer.class){
				if(singleInstance == null){
					singleInstance = new SqlDbServer();
				}
			}
		}
		
		return singleInstance;
	}
	public boolean startServer(){
		return super.startServer();
	}
}
