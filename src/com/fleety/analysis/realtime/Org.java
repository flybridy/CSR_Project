package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;
/*
 * 机构表
 * create by mike.li on 2014-02-27
 */
public class Org extends RedisTableBean{
    private String name;    //机构名称,父类UID存储机构ID
    private int  fid;       //父机构Id
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getFid() {
		return fid;
	}
	public void setFid(int fid) {
		this.fid = fid;
	}
    
    
}
