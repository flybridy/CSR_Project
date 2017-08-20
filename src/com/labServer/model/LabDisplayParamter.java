package com.labServer.model;

import java.util.Date;

public class LabDisplayParamter {

	private Integer id;
	private String inputProbeNumber;
	private String disProbeNumber;
	private String createdOn;
	private Double disTemperature;
	private Double disHumidity;
	private String displayTableName;

	public String getDisplayTableName() {
		return displayTableName;
	}

	public void setDisplayTableName(String displayTableName) {
		this.displayTableName = displayTableName;
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

	public String getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(String createdOn) {
		this.createdOn = createdOn;
	}

	public Double getDisTemperature() {
		return disTemperature;
	}

	public void setDisTemperature(Double disTemperature) {
		this.disTemperature = disTemperature;
	}

	public Double getDisHumidity() {
		return disHumidity;
	}

	public void setDisHumidity(Double disHumidity) {
		this.disHumidity = disHumidity;
	}

	@Override
	public String toString() {
		return "LabDisplayParamter [inputProbeNumber=" + inputProbeNumber + ", disProbeNumber=" + disProbeNumber + ", createdOn=" + createdOn + ", disTemperature=" + disTemperature + ", disHumidity=" + disHumidity + "]";
	}

	public LabDisplayParamter(String inputProbeNumber, String disProbeNumber, String createdOn, Double disTemperature, Double disHumidity) {
		super();
		this.inputProbeNumber = inputProbeNumber;
		this.disProbeNumber = disProbeNumber;
		this.createdOn = createdOn;
		this.disTemperature = disTemperature;
		this.disHumidity = disHumidity;
	}

	public LabDisplayParamter(String inputProbeNumber, String disProbeNumber, String createdOn, Double disTemperature, Double disHumidity, String displayTableName) {
		super();
		this.inputProbeNumber = inputProbeNumber;
		this.disProbeNumber = disProbeNumber;
		this.createdOn = createdOn;
		this.disTemperature = disTemperature;
		this.disHumidity = disHumidity;
		this.displayTableName = displayTableName;
	}

	public LabDisplayParamter() {
		super();
	}

}
