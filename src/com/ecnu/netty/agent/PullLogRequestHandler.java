package com.ecnu.netty.agent;


import com.ecnu.model.PullConstant;
import com.ecnu.netty.model.InvokeFuture;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.ResponseBuildler;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

/**
 * 拉取日志请求的处理器
 * @author zhujun
 *
 */
@Sharable
public class PullLogRequestHandler extends ChannelHandlerAdapter{

	private PullAgent agent;
	
	public PullLogRequestHandler(PullAgent agent) {
		this.agent = agent;
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelActive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		//super.channelRead(ctx, msg);
		if(msg instanceof PullRequest) {
			PullRequest request = (PullRequest)msg;
			if (request.getType() == PullConstant.PULL_LOG_REQUEST) {//拉取日志的请求
				
				String key = String.valueOf(request.getSno());
				if(agent.containsFuture(key)) {
					InvokeFuture<Object> future = agent.removeFuture(key);
					//没有找到对应的发送请求，则返回
					if (future != null) {
						future.setResult(msg);
					}
				}
				
				LoggerTool.info("pull log request, logFileName:" + request.getLogName() + " seq:" + request.getSeq() + " count:" + request.getCount(), new Throwable().getStackTrace());
				//根据日志文件名和偏移位置读取增量部分的日志内容
				
				//日志读取不到
				PullResponse response = ResponseBuildler.buildLogResponse(request, null);
				response.setCount(0);
				response.setSeq(0);
				response.setFlag(-1);
				this.agent.sendNoReturn(response);
				
				
			} else {
				//其他消息透传
				ctx.fireChannelRead(msg);
			}
		} else {
			//其他消息透传
			ctx.fireChannelRead(msg);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// TODO Auto-generated method stub
		LoggerTool.error(cause.getMessage(), cause.getStackTrace());
	}
	
}
