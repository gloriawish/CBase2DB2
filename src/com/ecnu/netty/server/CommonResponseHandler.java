package com.ecnu.netty.server;


import com.ecnu.model.PullConstant;
import com.ecnu.netty.listener.ResponseListener;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.tool.LoggerTool;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
@Sharable
public class CommonResponseHandler extends ChannelHandlerAdapter {
	
	@SuppressWarnings("unused")
	private PullServer server;
	public CommonResponseHandler(PullServer server) {
		this.server = server;
	}
	
	private ResponseListener responseListener;
	
	public void setResponseListener(ResponseListener responseListener) {
		this.responseListener = responseListener;
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
			if(response.getType() != PullConstant.PULL_LOG_RESPONSE) {
				if(responseListener != null)
					responseListener.onBaseInfoReceived(response, ctx.channel());
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
		cause.printStackTrace();
		LoggerTool.error(cause.getClass().getName(), new Throwable().getStackTrace());
	}
}
