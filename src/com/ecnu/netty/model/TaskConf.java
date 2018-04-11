package com.ecnu.netty.model;

public class TaskConf {

	private long lastSeq;
	private int logName;
	public long getLastSeq() {
		return lastSeq;
	}
	public void setLastSeq(long lastSeq) {
		this.lastSeq = lastSeq;
	}
	public int getLogName() {
		return logName;
	}
	public void setLogName(int logName) {
		this.logName = logName;
	}
}
