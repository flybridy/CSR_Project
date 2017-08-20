package com.fleety.job.video;

public class  OrgInfo{
	
	private String id;
	private String name;
	private String parentid;
	
	
	
	public OrgInfo(String id, String name, String parentid) {
		super();
		this.id = id;
		this.name = name;
		this.parentid = parentid;
	}
	public OrgInfo() {
		super();
	}
	@Override
	public String toString() {
		return "OrgInfo [id=" + id + ", name=" + name + ", parentid="
				+ parentid + "]";
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getParentid() {
		return parentid;
	}
	public void setParentid(String parentid) {
		this.parentid = parentid;
	}
	
	
}
