package com.ecnu.netty.server;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ecnu.model.AgentWrapper;
import com.ecnu.model.CDCConfig;
import com.ecnu.model.PullConstant;
import com.ecnu.model.SchemaWarpper;
import com.ecnu.netty.listener.ResponseListenerImpl;
import com.ecnu.netty.model.InvokeFuture;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.netty.model.ResolveTask;
import com.ecnu.netty.model.TaskConf;
import com.ecnu.serialize.RpcDecoder;
import com.ecnu.serialize.RpcEncoder;
import com.ecnu.sql.CBaseSQLBuilder;
import com.ecnu.sql.SqlGenerate;
import com.ecnu.sql.TableMap;
import com.ecnu.tool.ConfTool;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.ODError;
import com.ecnu.tool.ODRemoteConnector;
import com.ecnu.tool.Pair;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;

/**
 * 拉取服务器,负责拉取Agent的日志
 * @author Administrator
 *
 */
public class PullServer {
	
	public static boolean DEBUG = false;
	
	public static String SCHEMA_FILE_NAME = "./cdc_schema.dat";
	@Deprecated
	public static String TASK_FILE_NAME = "./cdc_task.dat";
	
	private InetSocketAddress inetAddr;
	
	@Deprecated
	private ODRemoteConnector rootServerConn;
	
	@Deprecated
	private String rootServerIp;
	
	@Deprecated
	private int rootServerPort;
	
	//表示和agent的连接通道
	@Deprecated
	private volatile AgentWrapper lastAgent;
	
	private Map<String, InvokeFuture<Object>> futrues = new ConcurrentHashMap<String, InvokeFuture<Object>>();
	//连接数组
	private Map<String, AgentWrapper> agents = new ConcurrentHashMap<String, AgentWrapper>();
	
	//schema缓存,实际上我们只需要一个份，不需要为每个Agent维护一份
	public Map<String, SchemaWarpper> schemaCache = new HashMap<String, SchemaWarpper>();
	
	
	public LinkedBlockingQueue<ResolveTask> responseQueue = new LinkedBlockingQueue<ResolveTask>(10000);
	
	private ServerBootstrap bootstrap;
	
	private long timeout = 3000;//默认超时
	
	private volatile boolean runing = false;
	
	private int delay = 1000;//每隔1000ms就拉取一次日志
	
	private int count;//一次拉取日志的数量
	
	private String savePath;
	
	private PullFromAllTask pullTask;
	
	private ResolveResponseTask resolveTask;
	
	private AgentWrapper contrlClient;//客户端
	
	private boolean showContrlLog;	//是否回传日志到客户端
	
	private TaskConf conf;			//pull任务的配置
	
	private Semaphore schemaLock = new Semaphore(1);
	
	private boolean needAppendSchema = false;
	
	private long startTime;
	private String startTimeStr;
	
	private CDCConfig startConfig;
	
	private boolean needAllAgentOnLine = true;
	
	PullServer() {
		
	}
	public PullServer(String host, int port, String rsIp, int rsPort,  String uname, String upass) {
		inetAddr=new InetSocketAddress(host,port);
		
		rootServerIp = rsIp;
		rootServerPort = rsPort;
		init();
		/*
		rootServerConn = new ODRemoteConnector(rsIp, uname, upass);
		
		if(rootServerConn.isConnectSuccess()) {
			init();
		} else {
			LoggerTool.error(String.format("Connect Root Server[%s] Error", rsIp), new Throwable().getStackTrace());
			System.exit(0);
		}
		*/
		
	}
	public PullServer(CDCConfig conf) {
		startConfig = conf;
		inetAddr=new InetSocketAddress(conf.getIp(),conf.getPort());
		
		rootServerIp = conf.getRootServer();
		rootServerPort = Integer.valueOf(conf.getRootServerPort());
		
		init();
		/*rootServerConn = new ODRemoteConnector(conf.getRootServer(), conf.getUserName(), conf.getUserPass());
		
		if(rootServerConn.isConnectSuccess()) {
			init();
		} else {
			LoggerTool.error(String.format("Connect Root Server[%s] Error", conf.getRootServer()), new Throwable().getStackTrace());
			System.exit(0);
		}*/
		
		if(!conf.isRestart()) {//不是重启的话就使用参数启动
			setConf(conf.getTaskConf());
			if(!conf.getSchemaPath().equals("null")) {//指定了initschema的话就不需要recover了
				initSchema(conf.getSchemaPath());
			}
		}
		setDelay(conf.getDelay());//设置拉取延迟
		setTimeOut(1000 * 50);//50秒延迟
		setCount(conf.getCount());//一次拉取日志的数量
		setSavePath(conf.getSavePath());
		
	}
	public long getTimeout() {
		return timeout;
	}
	@Deprecated
	public ODRemoteConnector getRootServerConn() {
		return rootServerConn;
	}
	
	public AgentWrapper getAgent(String key) {
		return agents.get(key);
	}

	public void putAgent(String key, AgentWrapper agent) {
		agents.put(key, agent);
	}
	
	public void removeAgent(String key) {
		agents.remove(key);
	}
	
	
	public void setConf(TaskConf conf) {
		this.conf = conf;
	}
	public void setLastAgent(AgentWrapper agent) {
		this.lastAgent = agent;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}
	
	public boolean isNeedAllAgentOnLine() {
		return needAllAgentOnLine;
	}
	public void setNeedAllAgentOnLine(boolean needAllAgentOnLine) {
		this.needAllAgentOnLine = needAllAgentOnLine;
	}
	public AgentWrapper getContrlClient() {
		return contrlClient;
	}
	public void setContrlClient(AgentWrapper contrlClient) {
		this.contrlClient = contrlClient;
	}
	public boolean isShowContrlLog() {
		return showContrlLog;
	}
	public void setShowContrlLog(boolean showContrlLog) {
		this.showContrlLog = showContrlLog;
	}
	public int getDelay() {
		return delay;
	}
	public void setSavePath(String savePath) {
		this.savePath = savePath;
	}
	public String getSavePath() {
		return savePath;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	public int getCount() {
		return count;
	}
	
	public Semaphore getSchemaLock() {
		return schemaLock;
	}
	
	public ResolveResponseTask getResolveTask() {
		return resolveTask;
	}
	private void init() {
		try {	
			
			final ControlHandler controlHandler = new ControlHandler(this);
			
			final LoginAuthHandler loginHandler = new LoginAuthHandler(this);
			
			final PullLogResponseHandler pullLogResponseHandler = new PullLogResponseHandler(this);
			
			final SchemaHandler schemaHandler = new SchemaHandler(this);
			
			final CommonResponseHandler commonHandler = new CommonResponseHandler(this);
			commonHandler.setResponseListener(new ResponseListenerImpl());
			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();
			bootstrap = new ServerBootstrap();
        	bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
           .handler(new LoggingHandler(LogLevel.INFO))
           .childHandler(new ChannelInitializer<SocketChannel>() {
                           @Override
                           public void initChannel(SocketChannel ch)
                                    throws Exception {
                            ch.pipeline().addLast(new RpcEncoder(PullRequest.class));
                            ch.pipeline().addLast(new RpcDecoder(PullResponse.class));
                            ch.pipeline().addLast(loginHandler);				//验证登录的处理器
                            ch.pipeline().addLast(new ReadTimeoutHandler(50));	//超时处理器
                            ch.pipeline().addLast(new HeartBeatHandler());		//心跳信息的处理器
                            ch.pipeline().addLast(schemaHandler);	//Schema处理器
                           	ch.pipeline().addLast(pullLogResponseHandler);		//日志信息的处理器
                           	ch.pipeline().addLast(controlHandler);				//控制命令
                           	ch.pipeline().addLast(commonHandler);				//通用的处理器
                          }
                     }).option(ChannelOption.SO_KEEPALIVE , true );
        } catch (Exception ex) {
        	ex.printStackTrace();
        }
	}
	
	public void start() {
		try {
			startTime = System.currentTimeMillis();
			Date date = new Date();
			SimpleDateFormat formart = new SimpleDateFormat("Y-M-d H:m:s");
			startTimeStr = formart.format(date);
			
			runing = true;
			ChannelFuture cfuture = bootstrap.bind(inetAddr.getPort()).sync();
			
			ExecutorService executorService=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			
			//恢复schema
			if(startConfig.isRestart())
				recoverSchema();
			
			//定时拉取日志的任务
			/*
			pullTask = new PullTask(this, savePath);
			
			if(conf != null) {
				pullTask.setTaskConfig(conf.getLastSeq(), conf.getLogName());
			} else {
				pullTask.recoverTask();
			}
			executorService.execute(pullTask);
			*/
			
			//new pull task (use mutator as item)
			
			if(startConfig.getFilter() != null && !startConfig.getFilter().equals("null")) {
				TableMap.load(startConfig.getFilter());
				LoggerTool.info("load filter", new Throwable().getStackTrace());
			}
			
			//设置SQL生成器
			SqlGenerate.SQLBUILDER = new CBaseSQLBuilder();
			
			pullTask = new PullFromAllTask(this, savePath);
			if(!startConfig.isRestart()) {
				pullTask.setTaskConfig(conf.getLastSeq(), conf.getLogName());
			} else {
				pullTask.recoverTask();
			}
			executorService.execute(pullTask);
			
			resolveTask = new ResolveResponseTask(this, pullTask, savePath);
			
			executorService.execute(resolveTask);
			
			//追加日志的任务
			if(needAppendSchema) {
				//conf必然不为null
				AppendSchemaTask schemaTask = new AppendSchemaTask(this);
				schemaTask.setLogName(conf.getLogName());
				schemaTask.setLastSeq(conf.getLastSeq());
				executorService.execute(schemaTask);
			}
			
			LoggerTool.server = this;
			cfuture.channel().closeFuture().sync();
		} catch (Exception e) {
			e.printStackTrace();
			runing = false;
		}
	}


	/**
	 * 获取主UPS的Agent
	 * @return
	 */
	@Deprecated
	public AgentWrapper getMasterAgent() {
		
		String cmd = String.format("rs_admin -r %s -p %s stat -o all_server", rootServerIp, rootServerPort);
		
		Pair<ODError, String> result = rootServerConn.executeValue(cmd);
		
		if(result.first == ODError.ERROR) {
			LoggerTool.error("rs_admin error:" + result.second, new Throwable().getStackTrace());
			return null;
		}
		
		//超时
		if (result.second.contains("timeout_mesg")) {
			
			LoggerTool.error("root server port maybe error! shell cmd:" + cmd, new Throwable().getStackTrace());
			System.exit(-1);
		} else {
			int endIndex = result.second.indexOf("chunkservers");
			int startIndex = result.second.indexOf("|");
			String str = result.second.substring(startIndex + 1,endIndex - 3);
			
			String[] ups = str.split(",");
			
			String master = "";
			
			for (int i = 0; i < ups.length; i++) {
				
				if(ups[i].contains("master")) {
					master = ups[i].substring(0,ups[i].indexOf(":"));
				}
			}
			
			LoggerTool.debug("master ip:" + master, new Throwable().getStackTrace());
			
			return agents.get(master);
		}
		
		return null;
	}
	@Deprecated
	public AgentWrapper getConfigMasterAgent() {
		return agents.get(startConfig.getMaster());
	}
	
	public Pair<List<AgentWrapper>, Integer> getConfigAgent() {
		List<AgentWrapper> list = new ArrayList<AgentWrapper>();
		boolean isComplete = true;
		for (String ip : startConfig.getUpsList()) {
			if(agents.containsKey(ip) && agents.get(ip).isAvailable()) {
				AgentWrapper ag = agents.get(ip);
				list.add(ag);
			} else {
				if(agents.containsKey(ip)) {					
					LoggerTool.warn("ups ip:" + ip + " is offline", new Throwable().getStackTrace());
				} else {
					LoggerTool.warn("ups ip:" + ip + " not registe", new Throwable().getStackTrace());
				}
				isComplete = false;
			}
		}
		if(needAllAgentOnLine) {
			if (isComplete) {
				return new Pair<List<AgentWrapper>, Integer>(list, 0);
			} else {
				return new Pair<List<AgentWrapper>, Integer>(list, -1);
			}
		} else {
			if(list.size() > 0) {
				return new Pair<List<AgentWrapper>, Integer>(list, 0);
			} else {
				return new Pair<List<AgentWrapper>, Integer>(list, -1);
			}
		}
	}
	
	/**
	 * 获取Agent
	 * @return
	 */
	@Deprecated
	public Pair<List<AgentWrapper>, Integer> getAgent() {
		
		List<AgentWrapper> list = new ArrayList<AgentWrapper>();
		
		String cmd = String.format("rs_admin -r %s -p %s stat -o all_server", rootServerIp, rootServerPort);
		
		Pair<ODError, String> result = rootServerConn.executeValue(cmd);
		
		if(result.first == ODError.ERROR) {
			LoggerTool.error("rs_admin error:" + result.second, new Throwable().getStackTrace());
			return new Pair<List<AgentWrapper>, Integer>(list, -1);
		}
		
		//超时
		if (result.second.contains("timeout_mesg")) {
			
			LoggerTool.error("root server port maybe error! shell cmd:" + cmd, new Throwable().getStackTrace());
			System.exit(-1);
		} else {
			int endIndex = result.second.indexOf("chunkservers");
			int startIndex = result.second.indexOf("|");
			String str = result.second.substring(startIndex + 1,endIndex - 3);
			
			String[] ups = str.split(",");
			
			String master = "";
			String ip = "";
			boolean isComplete = true;
			for (int i = 0; i < ups.length; i++) {
				ip = ups[i].substring(0,ups[i].indexOf(":"));
				if(ups[i].contains("master")) {
					master = ip;
					if(agents.containsKey(ip) && agents.get(ip).isAvailable()) {
						AgentWrapper ag = agents.get(ip);
						ag.setMaster(true);
						list.add(ag);
					} else {
						if(agents.containsKey(ip)) {
							LoggerTool.warn("master ups ip:" + ip + " is offline", new Throwable().getStackTrace());
						} else {
							LoggerTool.warn("master ups ip:" + ip + " not registe", new Throwable().getStackTrace());
						}
						isComplete = false;
					}
				} else {
					if(agents.containsKey(ip) && agents.get(ip).isAvailable()) {
						AgentWrapper ag = agents.get(ip);
						ag.setMaster(false);
						list.add(ag);
					} else {
						if(agents.containsKey(ip)) {
							LoggerTool.warn("slave ups ip:" + ip + " is offline", new Throwable().getStackTrace());
						} else {
							LoggerTool.warn("slave ups ip:" + ip + " not registe", new Throwable().getStackTrace());
						}
						
						
						isComplete = false;
					}
				}
			}
			LoggerTool.debug("master ip:" + master, new Throwable().getStackTrace());
			if (isComplete) {
				return new Pair<List<AgentWrapper>, Integer>(list, 0);
			} else {
				return new Pair<List<AgentWrapper>, Integer>(list, -1);
			}
		}
		return null;
	}
	
	/*
	public Object Send(PullRequest request,String agentAddress) {
		Channel channel=getAgent(agentAddress).getChannel();
		if(channel!=null) {	
			final InvokeFuture<Object> future=new InvokeFuture<Object>();
			futrues.put(String.valueOf(request.getSno()), future);
			//设置这次请求的ID
			future.setRequestId(String.valueOf(request.getSno()));
			ChannelFuture cfuture=channel.writeAndFlush(request);
			cfuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture rfuture) throws Exception {
					if(!rfuture.isSuccess()){
						future.setCause(rfuture.cause());
					}
				}
			});
			try {
				Object result=future.getResult(timeout, TimeUnit.MILLISECONDS);
				return result;
			}
			catch(RuntimeException e) {
				throw e;
			} finally {
				//这个结果已经收到
				futrues.remove(String.valueOf(request.getSno()));
			}
		} else {
			return null;
		}
	}*/
	
	public Object SendToAnyOnlineAgent(PullRequest request) {
		Pair<List<AgentWrapper>, Integer> p = getConfigAgent();//server.getAgent();
		List<AgentWrapper> agents = p.first;
		Channel channel = null;
		if(agents.size() > 0 && p.second == 0) {
			channel = agents.get(0).getChannel();
		}
		if(channel!=null) {	
			final InvokeFuture<Object> future=new InvokeFuture<Object>();
			futrues.put(String.valueOf(request.getSno()), future);
			//设置这次请求的ID
			future.setRequestId(String.valueOf(request.getSno()));
			ChannelFuture cfuture=channel.writeAndFlush(request);
			cfuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture rfuture) throws Exception {
					if(!rfuture.isSuccess()){
						future.setCause(rfuture.cause());
					}
				}
			});
			try {
				Object result=future.getResult(timeout, TimeUnit.MILLISECONDS);
				return result;
			}
			catch(RuntimeException e) {
				throw e;
			} finally {
				//这个结果已经收到
				futrues.remove(String.valueOf(request.getSno()));
			}
		} else {
			return null;
		}
	}
	
	@Deprecated
	public Object SendToMaster(PullRequest request) {
		Channel channel = null;
		AgentWrapper master = getMasterAgent();//获取主ups
		if (master != null && master.isAvailable())
			channel = master.getChannel(); 
		else
			LoggerTool.debug("no master", new Throwable().getStackTrace());
		if(channel!=null) {	
			final InvokeFuture<Object> future=new InvokeFuture<Object>();
			futrues.put(String.valueOf(request.getSno()), future);
			//设置这次请求的ID
			future.setRequestId(String.valueOf(request.getSno()));
			ChannelFuture cfuture=channel.writeAndFlush(request);
			cfuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture rfuture) throws Exception {
					if(!rfuture.isSuccess()){
						future.setCause(rfuture.cause());
					}
				}
			});
			try {
				Object result=future.getResult(timeout, TimeUnit.MILLISECONDS);
				return result;
			}
			catch(RuntimeException e) {
				throw e;
			} finally {
				//这个结果已经收到
				futrues.remove(String.valueOf(request.getSno()));
			}
		} else {
			return null;
		}
	}
	
	
	public InvokeFuture<Object> SendToAgent(PullRequest request, AgentWrapper agent) {
		Channel channel = null;
		if (agent != null && agent.isAvailable())
			channel = agent.getChannel(); 
		else
			LoggerTool.debug("agent is null", new Throwable().getStackTrace());
		if(channel!=null) {	
			final InvokeFuture<Object> future=new InvokeFuture<Object>();
			futrues.put(String.valueOf(request.getSno()), future);
			//设置这次请求的ID
			future.setRequestId(String.valueOf(request.getSno()));
			ChannelFuture cfuture=channel.writeAndFlush(request);
			cfuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture rfuture) throws Exception {
					if(!rfuture.isSuccess()){
						future.setCause(rfuture.cause());
					}
				}
			});
			return future;
		} else {
			throw new NullPointerException();
		}
	}
	
	@Deprecated
	public Object SendToLast(PullRequest request) {
		if(lastAgent == null || !lastAgent.isAvailable())
			return null;
		getMasterAgent();
		Channel channel = lastAgent.getChannel();
		if(channel != null) {	
			final InvokeFuture<Object> future=new InvokeFuture<Object>();
			futrues.put(String.valueOf(request.getSno()), future);
			//设置这次请求的ID
			future.setRequestId(String.valueOf(request.getSno()));
			ChannelFuture cfuture=channel.writeAndFlush(request);
			cfuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture rfuture) throws Exception {
					if(!rfuture.isSuccess()){
						future.setCause(rfuture.cause());
					}
				}
			});
			try {
				Object result=future.getResult(timeout, TimeUnit.MILLISECONDS);
				return result;
			}
			catch(RuntimeException e) {
				throw e;
			} finally {
				//这个结果已经收到
				futrues.remove(String.valueOf(request.getSno()));
			}
		} else {
			return null;
		}
	}
	/*
	public Object Send(PullRequest request,boolean async,String agentAddress) {
		Channel channel = getAgent(agentAddress).getChannel();
		if (channel != null) {	
			final InvokeFuture<Object> future=new InvokeFuture<Object>();
			futrues.put(String.valueOf(request.getSno()), future);
			ChannelFuture cfuture=channel.writeAndFlush(request);
			cfuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture rfuture) throws Exception {
					if(!rfuture.isSuccess()){
						future.setCause(rfuture.cause());
					}
				}
			});
			try {
				if (async) {//异步执行的话直接返回
					return null;
				}
				Object result=future.getResult(timeout, TimeUnit.MILLISECONDS);
				return result;
			} catch(RuntimeException e) {
				throw e;
			} finally {
				//这个结果已经收到
				if(!async)
					futrues.remove(String.valueOf(request.getSno()));
			}
		} else {
			return null;
		}
	}
	*/
	public void close() {
		for (Entry<String, AgentWrapper> ch : agents.entrySet()) {
			try {
				ch.getValue().getChannel().closeFuture().sync();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void recoverSchema() {
		try {
			File f = new File(SCHEMA_FILE_NAME);
			if(f.exists()) {
				RandomAccessFile randomFile = new RandomAccessFile(SCHEMA_FILE_NAME, "rw");
				SchemaWarpper schema = new SchemaWarpper();
				schema.setSeq(randomFile.readLong());
				
				int hostLen = randomFile.readInt();
				byte[] hostBuf = new byte[hostLen];
				randomFile.read(hostBuf);
				schema.setHost(new String(hostBuf));
				
				schema.setLogName(randomFile.readInt());
				
				int schemaLen= randomFile.readInt();
				byte[] schemaBuf = new byte[schemaLen];
				randomFile.read(schemaBuf);
				schema.setSchema(schemaBuf);
				schemaCache.put(PullConstant.SCHEMA_CACHE_UID, schema);
				randomFile.close();
				
				LoggerTool.info("recover schema info, seq:" + schema.getSeq() + " host:" + schema.getHost() + " logName:" + schema.getLogName() + " size:" + schema.getSchema().length , new Throwable().getStackTrace());
			} else {
				LoggerTool.warn("recover error, no schema file", new Throwable().getStackTrace());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 指定schema初始化
	 * @param schemaPath
	 */
	private void initSchema(String schemaPath) {
		try {
			//确保task.pos和schema.dat不存在
			File f1 = new File(SCHEMA_FILE_NAME);
			File f2 = new File(TASK_FILE_NAME);
			if(f1.exists()) {
				f1.delete();
			}
			if(f2.exists()) {
				f2.delete();
			}
			
			if(conf != null && conf.getLastSeq() != 0) {
				schemaLock.acquire();
				needAppendSchema = true;
			}
			
			Pattern regx = Pattern.compile("([0-9]*)_[0-9]-[0-9]_([0-9]*)\\.schema");
			Matcher m = regx.matcher(schemaPath);
			if(m.find()) {
				String logName = m.group(2);
				File f = new File(schemaPath);
				if(f.exists()) {
					RandomAccessFile randomFile = new RandomAccessFile(schemaPath, "rw");
					SchemaWarpper schema = new SchemaWarpper();
					schema.setSeq(0);
					schema.setHost("localhost");
					schema.setLogName(Integer.valueOf(logName));
					byte[] buf = new byte[(int) randomFile.length()];
					randomFile.read(buf);
					schema.setSchema(buf);
					randomFile.close();
					schemaCache.put(PullConstant.SCHEMA_CACHE_UID, schema);
					LoggerTool.info("init schema info, seq:" + schema.getSeq() + " host:" + schema.getHost() + " logName:" + schema.getLogName() + " size:" + schema.getSchema().length , new Throwable().getStackTrace());
					
					//保存一份schema，重启的时候使用
					saveSchema();
				} else {
					LoggerTool.warn("init schema file not found.", new Throwable().getStackTrace());
				}
			} else {
				LoggerTool.error("init schema file name not right.", new Throwable().getStackTrace());
				System.exit(-1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void saveSchema() {
		try {
			SchemaWarpper schema = schemaCache.get(PullConstant.SCHEMA_CACHE_UID);
			RandomAccessFile randomFile = new RandomAccessFile(PullServer.SCHEMA_FILE_NAME, "rw");
			
			
			LoggerTool.info("save schema info, seq:" + schema.getSeq() + " host:" + schema.getHost() + " logName:" + schema.getLogName() + " size:" + schema.getSchema().length , new Throwable().getStackTrace());
			
			randomFile.writeLong(schema.getSeq());
			randomFile.writeInt(schema.getHost().getBytes().length);
			randomFile.write(schema.getHost().getBytes());
			randomFile.writeInt(schema.getLogName());
			randomFile.writeInt(schema.getSchema().length);
			randomFile.write(schema.getSchema());
			randomFile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public boolean isClosed() {
		return runing == false;
	}
	
	public void stop() {
		runing = false;
	}
	
	public void goon() {
		runing = true;
	}
	
	public boolean containsFuture(String key) {
		return futrues.containsKey(key);
	}

	public InvokeFuture<Object> removeFuture(String key) {
		if(containsFuture(key))
			return futrues.remove(key);
		else
			return null;
	}
	public void setTimeOut(long timeout) {
		this.timeout=timeout;
	}
	
	public String summary() {
		StringBuilder sb = new StringBuilder();
		//AgentWrapper master = getConfigMasterAgent();//getMasterAgent();
		
		
		sb.append(" Server Start Time:" + startTimeStr);
		sb.append("\n");
		
		long curTime = System.currentTimeMillis();
		long runTime = curTime - startTime;
		sb.append(" Runing Time:" + (runTime / 1000) + "s\n");
		
		sb.append(" Agent Count:" + agents.size());
		sb.append("\n");
		for (Entry<String, AgentWrapper> item : agents.entrySet()) {
			sb.append(" Agent:" + item.getValue().getChannel().remoteAddress().toString() + " status:" + item.getValue().getStatus());
			sb.append("\n");
		}
		//sb.append(" Master Agent:" + (master == null ? "none" : master.getChannel().remoteAddress().toString()));
		sb.append(" Configure Agent:[");
		for (String ip : startConfig.getUpsList()) {
			sb.append(ip);
			sb.append(" ");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("]\n");
//		sb.append(" RootServer:" + rootServerIp + ":" + rootServerPort);
//		sb.append("\n");
		sb.append(" Local Address:" + inetAddr.toString());
		sb.append("\n");
		sb.append(" Restart Model:" + startConfig.isRestart());
		sb.append("\n");
		sb.append(" Pull Count:" + count);
		sb.append("\n");
		sb.append(" Pull Delay:" + delay);
		sb.append("\n");
		sb.append(" Time Out:" + timeout);
		sb.append("\n");
		sb.append(" Save Path:" + savePath);
		sb.append("\n");
		sb.append(" LastSeq:" + pullTask.getLastSeq());
		sb.append("\n");
		sb.append(" Log Name:" + pullTask.getLogName());
		sb.append("\n");
		sb.append(" Generate Sql Number:" + pullTask.getGenerateSqlNumber());
		sb.append("\n");
		
		SchemaWarpper schema = schemaCache.get(PullConstant.SCHEMA_CACHE_UID);
		if(schema != null) {
			sb.append(schema.toString());
			sb.append("\n");
		}
		
		sb.append(" Current Sql File:" + resolveTask.getCurrentFile());
		sb.append("\n");
		sb.append(" Cache Queue Size:" + responseQueue.size());
		sb.append("\n");
		if(conf != null) {
			sb.append(" Start Argument logName:" + conf.getLogName());
			sb.append("\n");
			sb.append(" Start Argument Seq:" + conf.getLastSeq());
			sb.append("\n");
		} else {
			sb.append(" Start Argument logName Default 1\n");
			sb.append(" Start Argument Seq Default 0\n");
		}
		
		return sb.toString();
		
	}
	
	public static String getIpAddress(SocketAddress address) {
		
		String str = address.toString();
		
		String ip = str.substring(1,str.indexOf(":"));
		
		return ip;
	}
	
	
	public static void main(String[] args) {
		/*
		if(args == null || args.length < 4) {
			System.out.println("arguments number error!");
			System.exit(0);
		}
		String ip = "127.0.0.1";
		int port = 8088;
		int delay = 1000;
		int level = 2;
		int count = 1;
		
		String rootIp = "127.0.0.1";
		int rootPort = 2500;
		String userName = "admin";
		String userPass = "admin";
		String savePath = "./";
		String initSchema = "";//启动初始化时的schema文件
		boolean useConf = false;
		TaskConf conf = new TaskConf();
		boolean restart = false;
		for (int i = 0; i < args.length-1; i++) {
			if(args[i].equals("-i")) {
				ip = args[i+1];
			}
			if(args[i].equals("-p")) {
				port = Integer.valueOf(args[i+1]);
			}
			if(args[i].equals("-t")) {
				delay = Integer.valueOf(args[i+1]);
			}
			if(args[i].equals("-l")) {
				level = Integer.valueOf(args[i+1]);
			}
			
			if(args[i].equals("-n")) {
				count = Integer.valueOf(args[i+1]);
			}
			
			if(args[i].equals("-r")) {
				rootIp = args[i+1];
			}
			
			if(args[i].equals("-rp")) {
				rootPort = Integer.valueOf(args[i+1]);
			}
			
			if(args[i].equals("-u")) {
				userName = args[i+1];
			}
			
			if(args[i].equals("-up")) {
				userPass = args[i+1];
			}
			
			if(args[i].equals("-f")) {
				savePath = args[i+1];
			}
			
			if(args[i].equals("-seq")) {
				conf.setLastSeq(Long.valueOf(args[i+1]));
				useConf= true;
			}
			
			if(args[i].equals("-log")) {
				conf.setLogName(Integer.valueOf(args[i+1]));
				useConf = true;
			}
			
			if(args[i].equals("-s")) {
				initSchema = args[i+1];
			}
			if(args[i].equals("-restart")) {
				if(args[i+1].equals("true")) {
					restart = true;
				}
			}
		}
		LoggerTool.LEVEL = level;
		final PullServer server = new PullServer(ip, port, rootIp, rootPort, userName, userPass);
		
		if(!restart) {//不是重启的话就使用参数启动
			if(useConf) {
				server.setConf(conf);
			}
			if(initSchema.length() > 0) {//指定了initschema的话就不需要recover了
				server.initSchema(initSchema);
			}
		}
		server.setDelay(delay);//设置拉取延迟
		server.setTimeOut(1000 * 50);//30秒延迟
		server.setCount(count);//一次拉取日志的数量
		server.setSavePath(savePath);
		server.start();
		*/
		
		CDCConfig startConf = ConfTool.loadConf("start.conf");
		LoggerTool.LEVEL = startConf.getLogLevel();
		final PullServer server = new PullServer(startConf);
		server.start();
		
	}

}
