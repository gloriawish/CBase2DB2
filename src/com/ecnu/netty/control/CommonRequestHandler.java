package com.ecnu.netty.control;

import com.ecnu.model.PullConstant;
import com.ecnu.netty.model.InvokeFuture;
import com.ecnu.netty.model.PullRequest;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
@Sharable
public class CommonRequestHandler extends ChannelHandlerAdapter {
	
	private ServerControl control;
	public CommonRequestHandler(ServerControl control) {
		this.control = control;
	}
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelActive(ctx);
	}

	
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelInactive(ctx);
		
		System.out.println("auto disconnect!");
		System.exit(0);
	}
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		// TODO Auto-generated method stub
		//super.channelRead(ctx, msg);
		if(msg instanceof PullRequest) {
			PullRequest request = (PullRequest)msg;
			if(request.getType() == PullConstant.CONTROL_RESULT || request.getType() == PullConstant.LOGIN_AUTH_RESULT) {
				
				String key = String.valueOf(request.getSno());
				if(control.containsFuture(key)) {
					InvokeFuture<Object> future = control.removeFuture(key);
					//没有找到对应的发送请求，则返回
					if (future != null) {
						future.setResult(msg);
					}
				}
			} else {
				//显示服务器回传的日志信息
				if(request.getType() == PullConstant.CONTROL_LOG) {
					
					System.out.println(new String(request.getExtend()));
					System.out.print("Input Command >:");
				}
				
				
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
		System.out.println("disconnect!");
	}
}
