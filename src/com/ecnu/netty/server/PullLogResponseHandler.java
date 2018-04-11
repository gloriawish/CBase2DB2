package com.ecnu.netty.server;

import com.ecnu.model.PullConstant;
import com.ecnu.netty.model.InvokeFuture;
import com.ecnu.netty.model.PullResponse;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
@Sharable
public class PullLogResponseHandler extends ChannelHandlerAdapter {

	private PullServer server;
	public PullLogResponseHandler(PullServer server) {
		this.server = server;
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
		if(msg instanceof PullResponse) {
			PullResponse response = (PullResponse)msg;
			if (response.getType() == PullConstant.PULL_LOG_RESPONSE || response.getType() == PullConstant.PULL_LOG_CRC_RESPONSE) {//日志信息
				
				String key = String.valueOf(response.getSno());
				if(server.containsFuture(key)) {
					InvokeFuture<Object> future = server.removeFuture(key);
					//没有找到对应的发送请求，则返回
					if (future != null) {
						future.setResult(msg);
					}
				}
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
