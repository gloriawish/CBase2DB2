package com.ecnu.netty.server;


import com.ecnu.model.AgentStatus;
import com.ecnu.model.AgentWrapper;
import com.ecnu.model.PullConstant;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.RequestBuildler;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
@Sharable
public class LoginAuthHandler extends ChannelHandlerAdapter {

	private PullServer server;
	public LoginAuthHandler(PullServer server) {
		this.server = server;
	}
	
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelInactive(ctx);
		//Agent断开连接后
		String key = PullServer.getIpAddress(ctx.channel().remoteAddress());
		if(server.getAgent(key) != null) {
			server.getAgent(key).setStatus(AgentStatus.OffLine);
			LoggerTool.info("Agent " + ctx.channel().remoteAddress() +" offline" , new Throwable().getStackTrace());
		}
	}


	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelActive(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// TODO Auto-generated method stub
		super.exceptionCaught(ctx, cause);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		// TODO Auto-generated method stub
		//super.channelRead(ctx, msg);
		if(msg instanceof PullResponse) {
			PullResponse response = (PullResponse)msg;
			String key = PullServer.getIpAddress(ctx.channel().remoteAddress());
			if (response.getType() == PullConstant.LOGIN_AUTH) {//授权登录
				
				LoggerTool.info("receive auth request from:" + ctx.channel().remoteAddress(), new Throwable().getStackTrace());
				
				if (response.getFlag() != PullConstant.CONTROL_CLEINT) {
					//添加到服务器的agent列表里
					AgentWrapper agent = new AgentWrapper(ctx.channel());
					agent.setStatus(AgentStatus.Disable);//验证后还不可用，必须得发送了Schema后才可以用
					this.server.putAgent(key, agent);//直接覆盖
					this.server.setLastAgent(agent);
				} else {//设置控制客户端
					if(server.getContrlClient()!= null) {
						server.getContrlClient().getChannel().close();//关闭与上一个控制台的连接
					}
					AgentWrapper contrlClient = new AgentWrapper(ctx.channel());
					contrlClient.setStatus(AgentStatus.OnLine);
					server.setContrlClient(contrlClient);
				}
				//发送登录成功消息
				PullRequest authResult = RequestBuildler.buildAuthSucessed(response);
				LoggerTool.info("send auth result, size:" + authResult.toBytes().length + " to:" +  ctx.channel().remoteAddress(), new Throwable().getStackTrace());
				ctx.writeAndFlush(authResult);
			} else {
				//其他消息透传
				ctx.fireChannelRead(msg);
			}
		} else {
			//其他消息透传
			ctx.fireChannelRead(msg);
		}
	}

}
