package com.ecnu.netty.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ecnu.model.AgentWrapper;
import com.ecnu.model.BytesWrapper;
import com.ecnu.model.PullConstant;
import com.ecnu.model.ResponseWrapper;
import com.ecnu.netty.model.InvokeFuture;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.netty.model.ResolveTask;
import com.ecnu.sql.SqlFileWriter;
import com.ecnu.sql.SqlGenerate;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.Pair;
import com.ecnu.tool.RequestBuildler;

public class PullFromAllTask implements Runnable {

	
	private PullServer server;
	
	@Deprecated
	private SqlFileWriter sw;
	
	private long lastSeq = 0;	//发送请求的时候使用
	
	private int logName = 1;
	
	private long realLastSeq = 0;//上一次的日志号，写文件的时候使用
	
	private long showLastSeq = 0;//用来日志显示的
	
	private long generateSqlNumber = 0;//启动到现在生成的sql数量
	
	public PullFromAllTask(PullServer server, String savePath) {
		this.server = server;
		//this.sw = new SqlFileWriter(savePath);
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
	
	public String getCurrentFile() {
		return sw.getCurrentFile();
	}
	
	
	public long getGenerateSqlNumber() {
		return generateSqlNumber;
	}

	public void increase() {
		generateSqlNumber++;
	}
	@SuppressWarnings("unused")
	@Deprecated
	private void saveTaskPos() {
		try {
			RandomAccessFile randomFile = new RandomAccessFile(PullServer.TASK_FILE_NAME, "rw");
			randomFile.writeLong(realLastSeq);
			randomFile.writeInt(logName);
			randomFile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void recoverTask() {
		/*
		try {
			File f = new File(PullServer.TASK_FILE_NAME);
			if(f.exists()) {
				RandomAccessFile randomFile = new RandomAccessFile(PullServer.TASK_FILE_NAME, "rw");
				lastSeq = randomFile.readLong();
				realLastSeq = lastSeq;
				showLastSeq = lastSeq;
				logName = randomFile.readInt();
				randomFile.close();
				LoggerTool.info("continue task, lastSeq:" + lastSeq +" logName:" + logName + " realLastSeq:" + realLastSeq, new Throwable().getStackTrace());
			} else {
				LoggerTool.info("task point not exists, create new task", new Throwable().getStackTrace());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		File root = new File(server.getSavePath());
		File[] files = root.listFiles();
		int maxId = 0;
		for (int i = 0; i < files.length; i++) {
			String id = files[i].getName().split("\\.")[0];
			if(maxId < Integer.parseInt(id)) {
				maxId = Integer.parseInt(id);
			}
		}
		//maxId--;
		//transactionend:123 [123,1]
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(server.getSavePath() + "/" + String.valueOf(maxId) + ".sql")));
			String line = null;
			long seq = 0;
			int fileid = 0;
			while ((line = br.readLine()) != null) {
				if(line.startsWith("transactionend:")) {
					String[] array = line.split(" ");
					seq = Long.parseLong(array[0].split(":")[1]);
					fileid = Integer.parseInt(array[1].split(":")[1]);
				}
			}
			
			lastSeq = seq;
			realLastSeq = lastSeq;
			showLastSeq = lastSeq;
			logName = fileid;
			LoggerTool.info("continue task, lastSeq:" + lastSeq +" logName:" + logName + " realLastSeq:" + realLastSeq, new Throwable().getStackTrace());
			br.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public void setTaskConfig(long lastSeq, int logName) {
		this.lastSeq = lastSeq;
		this.realLastSeq = lastSeq;
		this.showLastSeq = lastSeq;
		this.logName = logName;
	}
	
	@Override
	public void run() {
		try {
			server.getSchemaLock().acquire();
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}//获取信号量
		LoggerTool.info("start pull task", new Throwable().getStackTrace());
		while(true) {
			int count = server.getCount();//每次获取最新的值
			int delay = server.getDelay();
			long used = 0;
			if (!server.isClosed()) {
				String baseDir = "%s";
				//PullRequest request = RequestBuildler.buildPullLogRequest(String.format(baseDir, logName), lastSeq, count);
				PullResponse response = null;
				try {
					LoggerTool.debug("send pull request, logName:" + logName +" last_seq:" + showLastSeq + " count:" + count + " (request send lastSeq:" + lastSeq +")" , new Throwable().getStackTrace());
					
					//发送请求到所有的Agent，然后验证结果是否合法
					long start = System.currentTimeMillis();
					ResponseWrapper ret = sendRequestToAllAgent(String.format(baseDir, logName), lastSeq, count);
					used = System.currentTimeMillis() - start;
					
					if(ret.code == -1) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						continue;
					} else {
						response = ret.response;
					}
					
				} catch (Exception e) {
					e.printStackTrace();
					LoggerTool.warn("pull time out, logName:" + logName +" last_seq:" + showLastSeq + " count:" + count + " (request send lastSeq:" + lastSeq +")", new Throwable().getStackTrace());
					continue;
				}
				if(response != null) {
					//有数据
					if(response.getCount() > 0) {
						long start =System.currentTimeMillis();
						//耗时
						long temp = realLastSeq;
						//resolveResponse(response);
						pushTashToQueue(response);
						LoggerTool.info("pull time:" + (used) + "ms, push to queue time:" + (System.currentTimeMillis() - start) + "ms sno:" + response.getSno() + " logName:" + logName + " last_seq:" + showLastSeq + " log_seq:" + response.getSeq() + " temp:" + temp + "real_last:" + response.getRealSeq() + " count:" + response.getCount() + " flag:" + response.getFlag() + " (request lastSeq:" + lastSeq +")", new Throwable().getStackTrace());
						
					} else if (response.getFlag() == PullConstant.NORMAL) {
						LoggerTool.warn("empty response, request -> sno:" + response.getSno() +" logName:" + logName +" last_seq:" + showLastSeq + " count:" + count + " (request send lastSeq:" + lastSeq +")", new Throwable().getStackTrace());
					}
					//保存任务点
					//saveTaskPos();
					//根据传输过来的数据的flag进行处理
					lastSeq = response.getSeq();
					
					if(response.getSeq() > showLastSeq)
						showLastSeq = response.getSeq();//用来显示的，实际的lastSeq会再切换日志的时候变0，影响我们查看日志
					//1、切换日志
					if (response.getFlag() == PullConstant.OB_LOG_SWITCH_LOG) {
						logName++;
						lastSeq = 0;
						LoggerTool.info("switched log:" + logName, new Throwable().getStackTrace());
					}
					//2、日志文件不存在或者，读不到任何内容
					else if (response.getFlag() == PullConstant.LOG_END){//need wait data
						try {
							LoggerTool.debug("sleep some time." , new Throwable().getStackTrace());
							Thread.sleep(delay);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					//3、特殊情况
					else if (response.getFlag() == PullConstant.SEQ_INVALID) {
						//更新日志号
					}
					//4、Schema切换
					else if (response.getFlag() == PullConstant.SCHEMA_SWITCH ) {
						//更新日志号
					}
					//5、正常SQL
					else if (response.getFlag() == PullConstant.NORMAL) {
						//nothing todo
					}
					//6、Agent的Schema无效
					else if (response.getFlag() == PullConstant.SCHEMA_INVAILD) {
						//nothing todo
						//lastSeq = oldSeq;
						LoggerTool.warn("agent schema invalid or schema empty." , new Throwable().getStackTrace());
					}
					else if(response.getFlag() == PullConstant.AGENT_ERROR) {
						LoggerTool.warn("agent error, the log initial seq is bigger than your seq" , new Throwable().getStackTrace());
					}
					else if(response.getFlag() == PullConstant.BUFFER_OVERFLOW) {
						LoggerTool.warn("agent error, buffer overflow, please set count smaller", new Throwable().getStackTrace());
					}
					//5、异常
					else {
						throw new RuntimeException("flag unkown!");
					}
				}
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					break;
				}
			}
		}
		server.getSchemaLock().release();
	}
	
	/**
	 * 发送请求到Agent并验证返回结果是否合法
	 * @param request
	 * @return
	 */
	public ResponseWrapper sendRequestToAllAgent(String logName, long seq, int count) {
		//1、获取所有的agent
		PullRequest request = null;
		ResponseWrapper result = new ResponseWrapper();
		Pair<List<AgentWrapper>, Integer> p = server.getConfigAgent();//server.getAgent();
		List<AgentWrapper> agents = p.first;
		List<InvokeFuture<Object>> futures = new ArrayList<InvokeFuture<Object>>();
		if(agents.size() > 0 && p.second == 0) {
			//2、向所有的agent发送请求
			int d = (int) (System.currentTimeMillis() % agents.size());
			int idx = 0;
			for (AgentWrapper agentWrapper : agents) {
				request = RequestBuildler.buildPullLogRequest(logName, lastSeq, count);
				if(d == idx) {
					request.setType(PullConstant.PULL_LOG_REQUEST);
				} else {
					request.setType(PullConstant.PULL_LOG_CRC_REQUEST);
				}
				idx++;
//				if(agentWrapper.isMaster()) {
//					//2.2、向主UPS发送拉取日志的请求
//					request.setType(PullConstant.PULL_LOG_REQUEST);
//				} else {
//					//2.1、向备UPS发送拉取CRC的请求
//					request.setType(PullConstant.PULL_LOG_CRC_REQUEST);
//				}
				//2.3、通过netty发送到agent
				InvokeFuture<Object> f = server.SendToAgent(request, agentWrapper);
				if(f != null) {
					futures.add(f);
				} else {
					LoggerTool.error("future is null, agent is unavailable." , new Throwable().getStackTrace());
				}
			}
			List<PullResponse> reps = new ArrayList<PullResponse>();
			
			//3、获取所有的agent发回的结果
			for (InvokeFuture<Object> invokeFuture : futures) {
				PullResponse tmp = (PullResponse) invokeFuture.getResult(server.getTimeout(), TimeUnit.MILLISECONDS);
				reps.add(tmp);
			}
			
			//4、选出从UPS拉取到的日志结果,不是CRC日志
			for (PullResponse pullResponse : reps) {
				if(pullResponse.getType() == PullConstant.PULL_LOG_RESPONSE) {
					result.response = pullResponse;
					break;
				}
			}
			//5、判断数量是否一致
			for (int i = 1; i < reps.size(); i++) {
				if(reps.get(i).getCount() != reps.get(i - 1).getCount()) {
					LoggerTool.warn("response size not same ,retry pull." , new Throwable().getStackTrace());
					result.code = -1;
					return result;
				}
			}
			
			
			//6、获取UPS的日志的checksum
			List<String> standCheckSum = getCheckSumList(result.response);
			//7、验证checksum是否一致
			for (PullResponse pullResponse : reps) {
				if(pullResponse != result.response) {
					List<String> checkList = getCheckSumList(pullResponse);
					for (int i = 0; i < standCheckSum.size(); i++) {
						String v1 = standCheckSum.get(i);
						String v2 = checkList.get(i);
						if(!v1.equals(v2)) {//checksum不一致重试
							LoggerTool.warn("checksum not equal, retry pull." , new Throwable().getStackTrace());
							result.code = -1;
							return result;
						}
					}
				}
			}
			result.code = 0;
		} else {
			result.code = -1;
		}
		return result;
	}
	
	
	public void pushTashToQueue(PullResponse response) {
		
		
		ResolveTask task = new ResolveTask();
		task.setReponse(response);
		task.setLogName(logName);
		task.setRealLastSeq(realLastSeq);
		task.setShowLastSeq(showLastSeq);
		task.setLastSeq(lastSeq);
		try {
			server.responseQueue.put(task);
			LoggerTool.info("queue size:" + server.responseQueue.size() , new Throwable().getStackTrace());
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		realLastSeq = response.getRealSeq();
		/*
		//为了设置realLastSeq
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
				item.setLogName(String.valueOf(logName));
				item.setSeq(seq);
				cache.add(item);
			}
			realLastSeq = seq;
		}
		*/
	}
	
	/**
	 * 处理结果集
	 * @param response
	 */
	@Deprecated
	public void resolveResponse(PullResponse response) {
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
				item.setLogName(String.valueOf(logName));
				item.setSeq(seq);
				cache.add(item);
			}
			//3、生成一个事务的所有sql
			List<String> sqls = SqlGenerate.byteToSql(cache);
			//4、把事务写入文件
			sw.writeSingleSql("transactionbegin seq:" + seq + " last:" + realLastSeq);
			for (String string : sqls) {
				generateSqlNumber++;
				sw.writeSingleSql(string);
				//发送到控制台
				if (server != null && server.getContrlClient() != null && server.isShowContrlLog()) {
					PullRequest req = RequestBuildler.buildControlLog(string);
					server.getContrlClient().getChannel().writeAndFlush(req);
				}
			}
			sw.writeSingleSql("transactionend:" +seq + " file:" + logName);
			sw.checkNeedSwitchNextFile();//写完一个事务后在检查是否需要切换文件
			realLastSeq = seq;
		}
	}
	
	//获取checksum的列表
	public static List<String> getCheckSumList(PullResponse response) {
		List<String> result = new ArrayList<String>();
		String base = "%s(%s:%s)";
		
		if(response.getCount() > 0) {
			ByteBuffer buffer = ByteBuffer.allocate(response.getBody().length);
			buffer.put(response.getBody());
			buffer.flip();
			//分别读取每个mutator的数据
			while(buffer.remaining() > 0) {
				long seq = buffer.getLong();
				long checkSumBefore = buffer.getLong();
				long checkSumAfter = buffer.getLong();
				int num = buffer.getInt();
				
				for (int i = 0; i < num; i++) {
					buffer.getInt();//skip index
					int len = buffer.getInt();
					buffer.getLong();//skip seq
					
					byte[] logData = new byte[len];
					buffer.get(logData);
				}
				result.add(String.format(base, String.valueOf(seq),String.valueOf(checkSumBefore),String.valueOf(checkSumAfter)));
			}
		}
		return result;
	}

}
