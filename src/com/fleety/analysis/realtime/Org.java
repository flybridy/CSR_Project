package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;
/*
 * ������
 * create by mike.li on 2014-02-27
 */
public class Org extends RedisTableBean{
    private String name;    //��������,����UID�洢����ID
    private int  fid;       //������Id
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
