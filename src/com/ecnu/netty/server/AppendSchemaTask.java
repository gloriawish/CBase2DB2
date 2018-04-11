package com.ecnu.netty.server;


import com.ecnu.model.PullConstant;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.RequestBuildler;

public class AppendSchemaTask implements Runnable {

	
	private PullServer server;
	
	private long lastSeq = 0;
	
	private int logName = 1;
	
	public AppendSchemaTask(PullServer server) {
		this.server = server;
	}
	
	public long getLastSeq() {
		return lastSeq;
	}
	
	public void setLastSeq(long lastSeq) {
		this.lastSeq = lastSeq;
	}

	public void setLogName(int logName) {
		this.logName = logName;
	}

	public int getLogName() {
		return logName;
	}
	
	@Override
	public void run() {
		LoggerTool.info("start append schema task", new Throwable().getStackTrace());
		while(true) {
			if (!server.isClosed()) {
				String baseDir = "%s";
				PullRequest request = RequestBuildler.buildAppendSchemaRequest(String.format(baseDir, logName), lastSeq);
				PullResponse response = null;
				try {
					LoggerTool.debug("send schema append request, logName:" + logName +" last_seq:" + lastSeq, new Throwable().getStackTrace());
					//等待响应
					response = (PullResponse)server.SendToAnyOnlineAgent(request);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
				if(response != null) {
					if (response.getFlag() == PullConstant.NORMAL) {
						//开始 pull task
						server.getSchemaLock().release();
						LoggerTool.info("schema append sucess, sno:" + response.getSno() + " logName:" + logName + " last_seq:" + response.getSeq() + " read count:" + response.getCount() + " flag:" + response.getFlag(), new Throwable().getStackTrace());
						break;
					}
					else if (response.getFlag() == PullConstant.LOG_END){
						LoggerTool.warn("read log end", new Throwable().getStackTrace());
						break;
					}
					else if (response.getFlag() == PullConstant.OB_LOG_SWITCH_LOG) {
						LoggerTool.warn("switch log", new Throwable().getStackTrace());
						break;
					}
					//5、异常
					else {
						LoggerTool.error("flag unkown!", new Throwable().getStackTrace());
						throw new RuntimeException("flag unkown!");
					}
				}
				try {
					Thread.sleep(server.getDelay());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				break;
			}
		}
	}

}
