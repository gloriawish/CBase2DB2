package com.ecnu.netty.agent;

import com.ecnu.model.PullConstant;
import com.ecnu.netty.listener.RequestListener;
import com.ecnu.netty.model.InvokeFuture;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.tool.LoggerTool;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
@Sharable
public class CommonRequestHandler extends ChannelHandlerAdapter {
	
	private PullAgent agent;
	public CommonRequestHandler(PullAgent agent) {
		this.agent = agent;
	}
	
	
	private RequestListener requestListener;
	
	public void setRequestListener(RequestListener requestListener) {
		this.requestListener = requestListener;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelActive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		// TODO Auto-generated method stub
		//super.channelRead(ctx, msg);
		if(msg instanceof PullRequest) {
			PullRequest request = (PullRequest)msg;
			if(request.getType() != PullConstant.PULL_LOG_REQUEST) {
				
				String key = String.valueOf(request.getSno());
				if(agent.containsFuture(key)) {
					InvokeFuture<Object> future = agent.removeFuture(key);
					//没有找到对应的发送请求，则返回
					if (future != null) {
						future.setResult(msg);
					}
				}
				
				if(requestListener != null)
					requestListener.onRequestReceived(request, ctx.channel());
			} else {
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
		//super.exceptionCaught(ctx, cause);
		
		LoggerTool.error(cause.getMessage(), cause.getStackTrace());
	}
}
