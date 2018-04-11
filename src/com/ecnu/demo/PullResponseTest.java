package com.ecnu.demo;

import com.ecnu.netty.model.PullResponse;

public class PullResponseTest {

	
	public static void main(String[] args) {
		
		PullResponse req = new PullResponse();
		
		req.setSno(8001);
		req.setType(1001);
		req.setVersion(1);
		req.setBody(null);
		req.setSeq(1111);
		req.setRealSeq(9999);
		byte[] buf = req.toBytes();
		
		System.out.println("byte length:" + buf.length);
		
		
		PullResponse tmp = PullResponse.deserialize(buf);
		
		//System.out.println(new String(tmp.getBody()));
		
		System.out.println(tmp.getSno());
		System.out.println(tmp.getType());
		System.out.println(tmp.getVersion());
		System.out.println(tmp.getSeq());
		System.out.println(tmp.getRealSeq());
		
		
		
	}
}
