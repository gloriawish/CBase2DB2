package com.ecnu.netty.server;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.ecnu.model.BytesWrapper;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.netty.model.ResolveTask;
import com.ecnu.sql.SqlFileWriter;
import com.ecnu.sql.SqlGenerate;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.RequestBuildler;

public class ResolveResponseTask implements Runnable {

	
	private PullServer server;
	
	private SqlFileWriter sw;
	
	private PullFromAllTask pullTask;
	
	public ResolveResponseTask(PullServer server, PullFromAllTask task,String savePath) {
		this.server = server;
		this.pullTask = task;
		this.sw = new SqlFileWriter(savePath);
	}
	
	public String getCurrentFile() {
		return sw.getCurrentFile();
	}
	
	public String switchNewFile() {
		return sw.switchNewFile();
	}
	
	@Override
	public void run() {
		LoggerTool.info("start resolve schema task", new Throwable().getStackTrace());
		while(true) {
			try {
				ResolveTask task = server.responseQueue.take();
				long start = System.currentTimeMillis();
				resolveResponse(task);
				LoggerTool.info("resolve time:" + (System.currentTimeMillis() - start) + "ms sno:" + task.getReponse().getSno() + " logName:" + task.getLogName() + " last_seq:" + task.getShowLastSeq() + " log_seq:" + task.getReponse().getSeq() + " count:" + task.getReponse().getCount() + " flag:" + task.getReponse().getFlag() + " (request lastSeq:" + task.getLastSeq() +")", new Throwable().getStackTrace());
				
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	/**
	 * 处理结果集
	 * @param response
	 */
	public void resolveResponse(ResolveTask task) {
		PullResponse response = task.getReponse();
		long realLastSeq = task.getRealLastSeq();
		ByteBuffer buffer = ByteBuffer.allocate(response.getBody().length);
		buffer.put(response.getBody());
		buffer.flip();
		//1、分别读取每个mutator的数据
		while(buffer.remaining() > 0) {
			long seq = buffer.getLong();
			buffer.getLong();//skip checksum before
			buffer.getLong();//skip checksum after
			//2、组建一个mutator
			int num = buffer.getInt();//多少个cell info
			List<BytesWrapper> cache = new ArrayList<BytesWrapper>();
			for (int i = 0; i < num; i++) {
				buffer.getInt();//skip index
				int len = buffer.getInt();
				buffer.getLong();//skip seq
				byte[] logData = new byte[len];
				buffer.get(logData);
				BytesWrapper item = new BytesWrapper(logData);
				item.setLogName(String.valueOf(task.getLogName()));
				item.setSeq(seq);
				cache.add(item);
			}
			//3、生成一个事务的所有sql
			List<String> sqls = SqlGenerate.byteToSql(cache);
			//4、把事务写入文件
			sw.beginWrite();
			sw.writeSingleSql("transactionbegin seq:" + seq + " last:" + realLastSeq);
			for (String string : sqls) {
				pullTask.increase();
				sw.writeSingleSql(string);
				//发送到控制台
				if (server != null && server.getContrlClient() != null && server.isShowContrlLog()) {
					PullRequest req = RequestBuildler.buildControlLog(string);
					server.getContrlClient().getChannel().writeAndFlush(req);
				}
			}
			sw.writeSingleSql("transactionend:" +seq + " file:" + task.getLogName());
			sw.endWrite();
			sw.checkNeedSwitchNextFile();//写完一个事务后在检查是否需要切换文件
			realLastSeq = seq;
		}
	}
}
