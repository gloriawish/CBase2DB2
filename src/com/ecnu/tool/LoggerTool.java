package com.ecnu.tool;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.server.PullServer;

public class LoggerTool {

	public static int LEVEL = 3;
	
	public static PullServer server;
	
	public static void error(String info, StackTraceElement[] statck) {
		if(LEVEL > 0) {
			Date date = new Date();
			SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd H:mm:ss");
			String log = String.format("[%s] ERROR %s	\033[31m%s\033[0m", format.format(date), statck[0].getFileName() + ":" + statck[0].getLineNumber(), info);
			System.out.println(log);
			if (server != null && server.getContrlClient() != null) {
				PullRequest req = RequestBuildler.buildControlLog(log);
				server.getContrlClient().getChannel().writeAndFlush(req);
			}
		}
	}
	
	
	public static void warn(String info, StackTraceElement[] statck) {
		if(LEVEL > 1) {
			Date date = new Date();
			SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd H:mm:ss");
			String log = String.format("[%s] WARN %s \033[31m%s\033[0m", format.format(date), statck[0].getFileName() + ":" + statck[0].getLineNumber(), info);
			System.out.println(log);
			
			if (server != null && server.getContrlClient() != null) {
				PullRequest req = RequestBuildler.buildControlLog(log);
				server.getContrlClient().getChannel().writeAndFlush(req);
			}
		}
	}
	
	public static void info(String info, StackTraceElement[] statck) {
	
		if(LEVEL > 2) {
			Date date = new Date();
		
			SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd H:mm:ss");
			String log = String.format("[%s] INFO %s %s", format.format(date), statck[0].getFileName() + ":" + statck[0].getLineNumber(), info);
			System.out.println(log);
			if (server != null && server.isShowContrlLog()) {
				PullRequest req = RequestBuildler.buildControlLog(log);
				server.getContrlClient().getChannel().writeAndFlush(req);
			}

		}
		
	}
	
	
	public static void debug(String info, StackTraceElement[] statck) {
		if(LEVEL > 3) {
			Date date = new Date();
			SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd H:mm:ss");
			String log = String.format("[%s] DEBUG %s %s", format.format(date), statck[0].getFileName() + ":" + statck[0].getLineNumber(), info);
			System.out.println(log);
			if (server != null && server.isShowContrlLog()) {
				PullRequest req = RequestBuildler.buildControlLog(log);
				server.getContrlClient().getChannel().writeAndFlush(req);
			}
		}
	}
}
