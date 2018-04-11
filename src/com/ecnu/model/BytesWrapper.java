package com.ecnu.model;

/**
 * byte包裹类
 * @author zhujun
 *
 */
public class BytesWrapper {
	private byte[] bytes;
	private int length;
	private long seq;//日志号
	private String logName;//日志文件名
	public BytesWrapper(byte[] buffer) {
		this.bytes = buffer;
	}
	public byte[] getBytes() {
		return bytes;
	}
	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	
	
	
	public long getSeq() {
		return seq;
	}
	public void setSeq(long seq) {
		this.seq = seq;
	}
	public String getLogName() {
		return logName;
	}
	public void setLogName(String logName) {
		this.logName = logName;
	}
	
}
