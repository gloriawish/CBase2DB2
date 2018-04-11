package com.ecnu.netty.server;


import java.io.RandomAccessFile;

import com.ecnu.model.PullConstant;
import com.ecnu.model.SchemaWarpper;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.RequestBuildler;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
@Sharable
public class ControlHandler extends ChannelHandlerAdapter {

	private PullServer server;
	public ControlHandler(PullServer server) {
		this.server = server;
	}
	
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelInactive(ctx);
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
			if (response.getType() == PullConstant.CONTROL_SET_LEVEL) {
				
				String val = new String(response.getBody());
				LoggerTool.LEVEL = Integer.parseInt(val);
				LoggerTool.info("\033[33mreceive control log level request from:" + ctx.channel().remoteAddress() + " set log level=" + val + "\033[0m", new Throwable().getStackTrace());
				
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "sucess"));
				
			} else if (response.getType() == PullConstant.CONTROL_SET_COUNT) {
				
				String val = new String(response.getBody());
				server.setCount(Integer.parseInt(val));
				LoggerTool.info("\033[33mreceive control set count request from:" + ctx.channel().remoteAddress() + " set count=" + val + "\033[0m", new Throwable().getStackTrace());
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "sucess"));
			} else if (response.getType() == PullConstant.CONTROL_SET_DELAY) {
				
				String val = new String(response.getBody());
				server.setDelay(Integer.parseInt(val));
				LoggerTool.info("\033[33mreceive control set delay request from:" + ctx.channel().remoteAddress() + " set delay=" + val + "\033[0m", new Throwable().getStackTrace());
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "sucess"));
			}  else if (response.getType() == PullConstant.CONTROL_SHOW_LOG) {
				
				String val = new String(response.getBody());
				
				LoggerTool.info("\033[33mreceive control control show log request from:" + ctx.channel().remoteAddress() + "\033[0m", new Throwable().getStackTrace());
				if (val.equalsIgnoreCase("true")) {
					server.setShowContrlLog(true);
				} else {
					server.setShowContrlLog(false);
				}
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "sucess"));
			}  else if (response.getType() == PullConstant.CONTROL_STAT_REPORT) {
				
				//String val = new String(response.getBody());
				LoggerTool.info("\033[33mreceive control stat report request from:" + ctx.channel().remoteAddress() + "\033[0m", new Throwable().getStackTrace());
				
				String stat = server.summary();
				
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, stat));
			} else if (response.getType() == PullConstant.CONTROL_DUMP_SCHEMA) {
				
				String val = new String(response.getBody());
				LoggerTool.info("\033[33mreceive control dump schema request from:" + ctx.channel().remoteAddress() + " filename=" + val + "\033[0m", new Throwable().getStackTrace());
				
				SchemaWarpper schema = server.schemaCache.get(PullConstant.SCHEMA_CACHE_UID);
				RandomAccessFile randomFile = new RandomAccessFile(val, "rw");
				randomFile.writeLong(schema.getSeq());
				randomFile.writeInt(schema.getSchema().length);
				randomFile.write(schema.getSchema());
				randomFile.close();
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "success"));
			} else if (response.getType() == PullConstant.CONTROL_STOP) {
				
				LoggerTool.info("\033[33mreceive control shutdown request from:" + ctx.channel().remoteAddress() + "\033[0m", new Throwable().getStackTrace());
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "success"));
				
				Thread.sleep(1000);
				server.stop();
				LoggerTool.warn("PullServer has been stop.", new Throwable().getStackTrace());
			} else if (response.getType() == PullConstant.CONTROL_GOON) {
				
				LoggerTool.info("\033[33mreceive control goon request from:" + ctx.channel().remoteAddress() + "\033[0m", new Throwable().getStackTrace());
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "success"));
				server.goon();
				LoggerTool.warn("PullServer has been goon.", new Throwable().getStackTrace());
			} else if (response.getType() == PullConstant.CONTROL_KILL) {
				
				LoggerTool.info("\033[33mreceive control kill request from:" + ctx.channel().remoteAddress() + "\033[0m", new Throwable().getStackTrace());
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "success"));
				LoggerTool.warn("PullServer will kill after 10 seconds.", new Throwable().getStackTrace());
				server.stop();
				Thread.sleep(10000);
				System.exit(0);
			} else if (response.getType() == PullConstant.CONTROL_SWITCH) {
				LoggerTool.info("\033[33mreceive control switch request from:" + ctx.channel().remoteAddress() + "\033[0m", new Throwable().getStackTrace());
				ctx.writeAndFlush(RequestBuildler.buildControlResponse(response, "success"));
				String newFile = server.getResolveTask().switchNewFile();
				LoggerTool.info("switch to new file:" + newFile, new Throwable().getStackTrace());
			} 
			else {
				//其他消息透传
				ctx.fireChannelRead(msg);
			}
		} else {
			//其他消息透传
			ctx.fireChannelRead(msg);
		}
	}

}
