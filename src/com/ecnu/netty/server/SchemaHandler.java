package com.ecnu.netty.server;

import com.ecnu.model.AgentStatus;
import com.ecnu.model.PullConstant;
import com.ecnu.model.SchemaWarpper;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.RequestBuildler;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
@Sharable
public class SchemaHandler extends ChannelHandlerAdapter {

	private PullServer server;
	public SchemaHandler(PullServer server) {
		this.server = server;
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
			String schemakey = PullConstant.SCHEMA_CACHE_UID;
			if (response.getType() == PullConstant.SCHEMA_REPORT) {
				
				if(!server.schemaCache.containsKey(schemakey)) {
					
					SchemaWarpper schema = new SchemaWarpper();
					schema.setHost(key);
					schema.setLogName(response.getFlag());//flag标识日志名
					schema.setSeq(response.getSeq());
					schema.setSchema(response.getBody());
					server.schemaCache.put(schemakey, schema);
				} else {
					SchemaWarpper schema = server.schemaCache.get(schemakey);
					schema.setHost(key);
					schema.setLogName(response.getFlag());
					schema.setSeq(response.getSeq());
					schema.setSchema(response.getBody());
				}
				LoggerTool.info("##receive schema from:" + ctx.channel().remoteAddress() + " size:" + response.getBody().length + " version:" + response.getSeq(), new Throwable().getStackTrace());
				//SchemaWarpper schema = server.schemaCache.get(ctx.channel().remoteAddress().toString());
				//PullRequest result = RequestBuildler.buildSchemaResult(schema);
				//ctx.writeAndFlush(result);
				//持久化schema到文件
				server.saveSchema();
				
			} else if (response.getType() == PullConstant.SCHEMA_REQUIRE) {
				
				LoggerTool.info("require schema from: " +  ctx.channel().remoteAddress(), new Throwable().getStackTrace());
				try {
					SchemaWarpper schema = server.schemaCache.get(schemakey);
					
					if(schema != null) {
						LoggerTool.info("schema size:" + schema.getSchema().length, new Throwable().getStackTrace());
					} else {
						LoggerTool.warn("schema empty", new Throwable().getStackTrace());
					}
					
					PullRequest result = RequestBuildler.buildSchemaResult(schema);
					ctx.writeAndFlush(result);
					
					LoggerTool.info("send schema to agent:" +  ctx.channel().remoteAddress(), new Throwable().getStackTrace());
					
					//设置为在线状态
					server.getAgent(key).setStatus(AgentStatus.OnLine);
					
					LoggerTool.info("Agent " + ctx.channel().remoteAddress() +" online" , new Throwable().getStackTrace());
				} catch (Exception e) {
					e.printStackTrace();
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
	
	

}
