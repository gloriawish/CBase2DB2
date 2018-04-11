package com.ecnu.netty.model;

import java.nio.ByteBuffer;

public final class PullResponse {

	private int sno;		//序列号
	
	private int type;		//类型
	
	private int version;	//版本
	
	private int count;		//SQL条数
	
	private long seq;		//本次读取到的seq
	
	private long realSeq;		//本次读取到的seq

	private int flag;		//切换日志
	
	private byte[] body; 	//结果，可能多条SQL的内容，格式为|Length:4|SEQ:4|Data:N|...|

	public int getSno() {
		return sno;
	}

	public void setSno(int sno) {
		this.sno = sno;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}


	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}


	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}
	

	public long getRealSeq() {
		return realSeq;
	}

	public void setRealSeq(long realSeq) {
		this.realSeq = realSeq;
	}

	/**
	 * 序列化对象
	 * @return
	 */
	public byte[] toBytes() {
		//sno:4 | type:4 | version:4 | count:4 | flag:4 | seq:8 | real_seq:8 | length:8 | body:n| 
		int size = 4 + 4 + 4 + 4 + 4+ 8 + 8 + 8;
		if(body != null) {
			size += body.length;
		}
		ByteBuffer buffer = ByteBuffer.allocate(size);
		buffer.putInt(sno);//sno
		buffer.putInt(type);//type
		buffer.putInt(version);//version
		buffer.putInt(count);//count
		buffer.putInt(flag);//flag
		buffer.putLong(seq);//seq
		buffer.putLong(realSeq);//real seq
		if(body != null) {
			buffer.putLong(body.length);//length
			buffer.put(body);//body
		} else {
			buffer.putLong(0);//length
		}
		
		return buffer.array();
	}
	
	/**
	 * 反序列化对象
	 * @param buf
	 * @return
	 */
	public static PullResponse deserialize(byte[] buf) {
		ByteBuffer buffer = ByteBuffer.allocate(buf.length);
		buffer.put(buf);
		buffer.flip();

		PullResponse response = new PullResponse();
		response.sno = buffer.getInt();
		response.type = buffer.getInt();
		response.version = buffer.getInt();
		response.count = buffer.getInt();
		response.flag = buffer.getInt();
		response.seq = buffer.getLong();
		response.realSeq = buffer.getLong();
		int length = (int)buffer.getLong();
		if(length > 0) {
			byte[] body = new byte[length];
			buffer.get(body, 0, length);
			response.body = body;
		}
		return response;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("sno:" + sno);
		sb.append(" ");
		sb.append("type:" + type);
		sb.append(" ");
		sb.append("count:" + count);
		sb.append(" ");
		sb.append("seq:" + seq);
		sb.append(" ");
		sb.append("flag:" + flag);
		if(body != null) {
			sb.append(" ");
			sb.append("szie:" + body.length);
		}
		
		return sb.toString();
	}
	
}
