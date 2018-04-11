package com.ecnu.netty.server;

import com.ecnu.model.PullConstant;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.tool.LoggerTool;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class HeartBeatHandler extends ChannelHandlerAdapter{

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelActive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		// TODO Auto-generated method stub
		//super.channelRead(ctx, msg);
		if(msg instanceof PullResponse) {
			PullResponse response = (PullResponse)msg;
			if (response.getType() == PullConstant.HEART_BEAT) {//心跳信息

				LoggerTool.debug("receive heart beat from:" + ctx.channel().remoteAddress(), new Throwable().getStackTrace());
				
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
		super.exceptionCaught(ctx, cause);
	}
}
