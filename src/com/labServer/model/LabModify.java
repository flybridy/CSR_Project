package com.labServer.model;

import java.util.Date;

public class LabModify {

	private Integer id;
	private String inputProbeNumber;
	private String disProbeNumber;
	private String modifyOn;
	private String createdOn;
	private String stopEnd;
	private Double modifyTemp;
	private Double modifyHum;
	private String Status;
	private String name;

	public Double getModifyTemp() {
		return modifyTemp;
	}

	public void setModifyTemp(Double modifyTemp) {
		this.modifyTemp = modifyTemp;
	}

	public Double getModifyHum() {
		return modifyHum;
	}

	public void setModifyHum(Double modifyHum) {
		this.modifyHum = modifyHum;
	}

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

	public String getDisProbeNumber() {
		return disProbeNumber;
	}

	public void setDisProbeNumber(String disProbeNumber) {
		this.disProbeNumber = disProbeNumber;
	}

	public String getModifyOn() {
		return modifyOn;
	}

	public void setModifyOn(String modifyOn) {
		this.modifyOn = modifyOn;
	}

	public String getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(String createdOn) {
		this.createdOn = createdOn;
	}

	public String getStopEnd() {
		return stopEnd;
	}

	public void setStopEnd(String stopEnd) {
		this.stopEnd = stopEnd;
	}

	public String getStatus() {
		return Status;
	}

	public void setStatus(String status) {
		Status = status;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
