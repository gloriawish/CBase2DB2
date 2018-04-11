package com.ecnu.tool;

import java.util.concurrent.atomic.AtomicInteger;

import com.ecnu.model.PullConstant;
import com.ecnu.model.SchemaWarpper;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;

public class RequestBuildler {
	
	private static AtomicInteger requestId=new AtomicInteger(0);//消息的ID
	private static int VERSION = 1;
	
	/**
	 * 构建验证参数
	 * @param name
	 * @return
	 */
	public static PullRequest buildAuthSucessed(PullResponse response) {
		PullRequest request = new PullRequest();
		request.setSno(response.getSno());
		request.setType(PullConstant.LOGIN_AUTH_RESULT);
		request.setVersion(VERSION);
		request.setLogName(PullConstant.EMPTYLOG);
		request.setSeq(0);
		return request;
	}
	
	
	/**
	 * 构建日志请求参数
	 * @param logName
	 * @param offset
	 * @return
	 */
	public static PullRequest buildPullLogRequest(String logName, long seq, int count) {
		PullRequest request = new PullRequest();
		request.setSno(requestId.getAndIncrement());
		request.setType(PullConstant.PULL_LOG_REQUEST);
		request.setVersion(VERSION);
		request.setLogName(logName);
		request.setSeq(seq);
		request.setCount(count);
		return request;
	}
	
	
	public static PullRequest buildAppendSchemaRequest(String logName, long seq) {
		PullRequest request = new PullRequest();
		request.setSno(requestId.getAndIncrement());
		request.setType(PullConstant.SCHEMA_APPEND);
		request.setVersion(VERSION);
		request.setLogName(logName);
		request.setSeq(seq);
		request.setCount(0);
		return request;
	}
	
	public static PullRequest buildSchemaResult(SchemaWarpper schema) {
		PullRequest request = new PullRequest();
		request.setSno(requestId.getAndIncrement());
		request.setType(PullConstant.SCHEMA_RESULT);
		request.setVersion(VERSION);
		request.setCount(0);
		if (schema != null) {
			request.setLogName(String.valueOf(schema.getLogName()));
			request.setSeq(schema.getSeq());
			request.setExtend(schema.getSchema());
		} else {
			request.setLogName("1");
			request.setSeq(-1);
			request.setExtend(null);
		}
		return request;
		
	}
	
	public static PullRequest buildControlResponse(PullResponse response, String message) {
		PullRequest request = new PullRequest();
		request.setSno(response.getSno());
		request.setType(PullConstant.CONTROL_RESULT);
		request.setVersion(VERSION);
		request.setLogName(PullConstant.EMPTYLOG);
		request.setSeq(0);
		request.setExtend(message.getBytes());
		return request;
	}
	
	public static PullRequest buildControlLog(String message) {
		PullRequest request = new PullRequest();
		request.setSno(requestId.getAndIncrement());
		request.setType(PullConstant.CONTROL_LOG);
		request.setVersion(VERSION);
		request.setLogName(PullConstant.EMPTYLOG);
		request.setSeq(0);
		request.setExtend(message.getBytes());
		return request;
	}
	

}
