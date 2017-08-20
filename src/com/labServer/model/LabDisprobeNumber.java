package com.labServer.model;

import java.util.Date;

public class LabDisprobeNumber {

	private Integer id;
	private String inputProbeNumber;
	private String displayProbeNumber;
	private String create_time;
	private String tab_InputName;
	private String tab_DisplayName;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getInputProbeNumber() {
		return inputProbeNumber;
	}

	public void setInputProbeNumber(String inputProbeNumber) {
		this.inputProbeNumber = inputProbeNumber;
	}

	public String getDisplayProbeNumber() {
		return displayProbeNumber;
	}

	public void setDisplayProbeNumber(String displayProbeNumber) {
		this.displayProbeNumber = displayProbeNumber;
	}

	public String getCreate_time() {
		return create_time;
	}

	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}

	public String getTab_InputName() {
		return tab_InputName;
	}

	public void setTab_InputName(String tab_InputName) {
		this.tab_InputName = tab_InputName;
	}

	public String getTab_DisplayName() {
		return tab_DisplayName;
	}

	public void setTab_DisplayName(String tab_DisplayName) {
		this.tab_DisplayName = tab_DisplayName;
	}

}
