package com.ecnu.tool;

import java.util.concurrent.atomic.AtomicInteger;

import com.ecnu.model.PullConstant;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;

public class ResponseBuildler {
	
	private static AtomicInteger requestId=new AtomicInteger(0);//消息的ID
	private static int VERSION = 1;
	
	/**
	 * 构建验证参数
	 * @param name
	 * @return
	 */
	public static PullResponse buildAuth(String name) {
		PullResponse response = new PullResponse();
		response.setSno(requestId.getAndIncrement());
		response.setType(PullConstant.LOGIN_AUTH);
		response.setVersion(VERSION);
		response.setBody(name.getBytes());
		return response;
	}
	
	/**
	 * 构建日志响应
	 * @param request
	 * @param buf
	 * @return
	 */
	public static PullResponse buildLogResponse(PullRequest request, byte[] buf) {
		PullResponse response = new PullResponse();
		response.setSno(request.getSno());
		response.setType(PullConstant.PULL_LOG_RESPONSE);
		response.setVersion(VERSION);
		response.setBody(buf);
		return response;
	}
	
	
	/**
	 * 构建心跳包
	 * @param address
	 * @return
	 */
	public static PullResponse buildHeartBeat(String address) {
		PullResponse response = new PullResponse();
		response.setSno(requestId.getAndIncrement());
		response.setType(PullConstant.HEART_BEAT);
		response.setVersion(VERSION);
		response.setBody(address.getBytes());
		response.setFlag(PullConstant.CONTROL_CLEINT);
		return response;
	}

	
	public static PullResponse buildControl(int type, String cmd) {
		PullResponse response = new PullResponse();
		response.setSno(requestId.getAndIncrement());
		response.setType(type);
		response.setVersion(VERSION);
		response.setBody(cmd.getBytes());
		response.setFlag(PullConstant.CONTROL_CLEINT);
		return response;
	}
}
