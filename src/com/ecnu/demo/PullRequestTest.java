package com.ecnu.demo;

import com.ecnu.netty.model.PullRequest;
import com.ecnu.tool.ResponseBuildler;

public class PullRequestTest {

	
	public static void main(String[] args) {
		
		PullRequest req = new PullRequest();
		
		req.setSno(8001);
		req.setType(1001);
		req.setLogName("test_log");
		req.setVersion(1);
		req.setSeq(2048);
		
		byte[] buf = req.toBytes();
		
		System.out.println("byte length:" + buf.length);
		
		
		PullRequest tmp = PullRequest.deserialize(buf);
		
		System.out.println(tmp.getLogName());
		
		
		System.out.println(ResponseBuildler.buildAuth("agent").toBytes().length);
		
		
		System.out.println(tmp.getSeq());
		
	}
}
