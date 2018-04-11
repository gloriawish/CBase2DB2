package com.ecnu.netty.model;

public class ResolveTask {
	
	private PullResponse reponse;
	
	private int logName;
	
	private long realLastSeq;
	
	private long showLastSeq;
	
	private long lastSeq;

	public PullResponse getReponse() {
		return reponse;
	}

	public void setReponse(PullResponse reponse) {
		this.reponse = reponse;
	}

	public int getLogName() {
		return logName;
	}

	public void setLogName(int logName) {
		this.logName = logName;
	}

	public long getRealLastSeq() {
		return realLastSeq;
	}

	public void setRealLastSeq(long realLastSeq) {
		this.realLastSeq = realLastSeq;
	}

	public long getShowLastSeq() {
		return showLastSeq;
	}

	public void setShowLastSeq(long showLastSeq) {
		this.showLastSeq = showLastSeq;
	}

	public long getLastSeq() {
		return lastSeq;
	}

	public void setLastSeq(long lastSeq) {
		this.lastSeq = lastSeq;
	}
	
	

}
