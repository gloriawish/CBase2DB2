package com.ecnu.netty.listener;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ecnu.model.PullConstant;
import com.ecnu.netty.agent.PullAgent;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.ResponseBuildler;

import io.netty.channel.Channel;

public class RequestListenerImpl extends RequestListener{

	private PullAgent agent;
	public RequestListenerImpl(PullAgent agent) {
		this.agent = agent;
	}
	
	private ScheduledExecutorService timerServer = null;
	
	public void onRequestReceived(PullRequest msg, Channel channel) {
		
		final String address = channel.localAddress().toString();
		//如果收到的为验证通过的包，则启动心跳发送
		if(msg.getType() == PullConstant.LOGIN_AUTH_RESULT) {
			
			LoggerTool.info(address + " auth sucess", new Throwable().getStackTrace());
			if (timerServer != null) {
				timerServer.shutdown();
				timerServer = null;
			}
			if (timerServer == null){
				timerServer = Executors.newScheduledThreadPool(1);
			}
		    timerServer.scheduleWithFixedDelay(new Runnable() {
				@Override
				public void run() {
					LoggerTool.info("send heart beat.", new Throwable().getStackTrace());
					agent.sendNoReturn(ResponseBuildler.buildHeartBeat(address));
				}
			}, 10, 30, TimeUnit.SECONDS);
		}
		
	}
}
