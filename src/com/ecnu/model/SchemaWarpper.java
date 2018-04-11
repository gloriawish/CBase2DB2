package com.ecnu.model;

public class SchemaWarpper {

	private String host;
	
	private int logName;
	
	private long seq;
	
	private byte[] schema;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public byte[] getSchema() {
		return schema;
	}

	public void setSchema(byte[] schema) {
		this.schema = schema;
	}

	public int getLogName() {
		return logName;
	}

	public void setLogName(int logName) {
		this.logName = logName;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(" schema info:\n");
		sb.append("\t host:" + host + "\n");
		sb.append("\t log id:" + logName + "\n");
		sb.append("\t seq:" + seq + "\n");
		sb.append("\t size:" + schema.length);
		return sb.toString();
	}
	
}
