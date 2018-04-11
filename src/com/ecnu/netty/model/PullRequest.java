package com.ecnu.netty.model;

import java.nio.ByteBuffer;

public final class PullRequest {

	private int sno;		//序列号
	
	private int type;		//类型
	
	private String logName; //日志文件名
	
	private long seq;		//日志号
	
	private int version;	//版本
	
	private int count;		//数量
	
	private byte[] extend;	//额外字段

	public int getSno() {
		return sno;
	}

	public void setSno(int sno) {
		this.sno = sno;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getLogName() {
		return logName;
	}

	public void setLogName(String logName) {
		this.logName = logName;
	}

	

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}
	
	
	public byte[] getExtend() {
		return extend;
	}

	public void setExtend(byte[] extend) {
		this.extend = extend;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	/**
	 * 序列化
	 * @return
	 */
	public byte[] toBytes() {
		if(logName == null)
			return null;
		// sno:4 | type:4 | length:4 | logName:n | seq:8 | version:4 | count:4 | extendLength:4 | extend:n |
		int size = 4 + 4 + 4 + logName.getBytes().length + 8 + 4 + 4 + 4;
		if(extend != null)
			size += extend.length;
		ByteBuffer buffer = ByteBuffer.allocate(size);
		buffer.putInt(sno);//sno
		buffer.putInt(type);//type
		buffer.putInt(logName.getBytes().length);//logName length
		buffer.put(logName.getBytes());//logName
		buffer.putLong(seq);//seq
		buffer.putInt(version);//version
		buffer.putInt(count);//count
		
		if (extend == null) {
			buffer.putInt(0);
		} else {
			buffer.putInt(extend.length);
			buffer.put(extend);
		}
		return buffer.array();
	}
	
	/**
	 * 反序列化
	 * @param buf
	 * @return
	 */
	public static PullRequest deserialize(byte[] buf) {
		ByteBuffer buffer = ByteBuffer.allocate(buf.length);
		buffer.put(buf);
		buffer.flip();

		PullRequest request = new PullRequest();
		request.sno = buffer.getInt();
		request.type = buffer.getInt();
		int length = buffer.getInt();
		byte[] temp = new byte[length];
		buffer.get(temp, 0, length);
		request.logName = new String(temp);
		request.seq = buffer.getLong();
		request.version= buffer.getInt();
		request.count= buffer.getInt();
		
		int extendLength = buffer.getInt();
		if(extendLength > 0) {
			byte[] extend = new byte[extendLength];
			buffer.get(extend);
			request.extend = extend;
		}
		return request;
	}
	
}
