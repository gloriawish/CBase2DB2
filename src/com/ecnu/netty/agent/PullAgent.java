package com.ecnu.netty.agent;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ecnu.netty.listener.RequestListenerImpl;
import com.ecnu.netty.model.InvokeFuture;
import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;
import com.ecnu.serialize.RpcDecoder;
import com.ecnu.serialize.RpcEncoder;
import com.ecnu.tool.LoggerTool;
import com.ecnu.tool.ResponseBuildler;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * 代理客户端
 * @author zhujun
 *
 */
public class PullAgent {
	
	private InetSocketAddress inetAddr;
	
	private volatile Channel channel;
	
	private Map<String, InvokeFuture<Object>> futrues=new ConcurrentHashMap<String, InvokeFuture<Object>>();
	//与Server连接数组
	private Map<String, Channel> channels=new ConcurrentHashMap<String, Channel>();
	
	private Bootstrap bootstrap;
	
	private long timeout=10000;//默认超时
	
	private boolean connected=false;
	
	private boolean isInitialize = false;
	
	private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	
	PullAgent() {
		
	}
	public PullAgent(String host,int port) {
		inetAddr=new InetSocketAddress(host,port);
	}

	private Channel getChannel(String key) {
		return channels.get(key);
	}
	public void init() {
		if(!isInitialize) {
			try {
				final PullLogRequestHandler pullLogHandler = new PullLogRequestHandler(this);
				final CommonRequestHandler requestHandler = new CommonRequestHandler(this);
				requestHandler.setRequestListener(new RequestListenerImpl(this));
				EventLoopGroup group = new NioEventLoopGroup();
	            bootstrap = new Bootstrap();
	            bootstrap.group(group).channel(NioSocketChannel.class)
	                .handler(new ChannelInitializer<SocketChannel>() {
	                    @Override
	                    public void initChannel(SocketChannel channel) throws Exception {
	                    	channel.pipeline().addLast(new RpcDecoder(PullRequest.class));
	                    	channel.pipeline().addLast(new RpcEncoder(PullResponse.class));
	                        channel.pipeline().addLast(pullLogHandler);
	                        channel.pipeline().addLast(requestHandler);
	                    }
	                })
	                .option(ChannelOption.SO_KEEPALIVE, true);
	            this.isInitialize = true;
	        } catch (Exception ex) {
	        	ex.printStackTrace();
	        }
		}
	}

	public void connect() {
		init();
		try {
			ChannelFuture future = bootstrap.connect(this.inetAddr).sync();
			this.channel = future.channel();
			channels.put(this.inetAddr.toString(), future.channel()); 
			connected=true;
			
			LoggerTool.debug("connected", (new Throwable()).getStackTrace());
			//发送验证登录信息
			PullResponse authReponse = ResponseBuildler.buildAuth(this.inetAddr.toString());
			Send(authReponse, false);
			LoggerTool.debug("auth sucess!", (new Throwable()).getStackTrace());
			
			future.channel().closeFuture().sync();
			
		} catch(InterruptedException e) {
			e.printStackTrace();
		} finally {
			channels.clear();
			futrues.clear();
			channel = null;
			connected = false;
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						TimeUnit.SECONDS.sleep(5);
						try {
							//重连操作
							LoggerTool.info("reconnect server", new Throwable().getStackTrace());
							connect();
						} catch (Exception ex) {
							
						}
					} catch (Exception ex) {
						
					}
				}
			});
			
		}
	}

	@Deprecated
	public void connect(String host, int port) {
		ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
		future.addListener(new ChannelFutureListener(){
			@Override
			public void operationComplete(ChannelFuture cfuture) throws Exception {
				 Channel channel = cfuture.channel();
				 //添加进入连接数组
			     channels.put(channel.remoteAddress().toString(), channel); 
			}
		});
	}

	@Deprecated
	public Object Send(PullResponse response) {//同步发送消息给服务器
		if(channel==null)
			channel=getChannel(inetAddr.toString());
		if(channel!=null) {	
			final InvokeFuture<Object> future=new InvokeFuture<Object>();
			futrues.put(String.valueOf(response.getSno()), future);
			//设置这次请求的ID
			future.setRequestId(String.valueOf(response.getSno()));
			ChannelFuture cfuture=channel.writeAndFlush(response);
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
			} catch(RuntimeException e) {
				throw e;
			} finally {
				//这个结果已经收到
				futrues.remove(String.valueOf(response.getSno()));
			}
		} else {
			return null;
		}
	}
	
	public Object Send(PullResponse response,boolean async) {
		if (channel == null)
			channel = getChannel(inetAddr.toString());
		if (channel != null) {	
			final InvokeFuture<Object> future=new InvokeFuture<Object>();
			futrues.put(String.valueOf(response.getSno()), future);
			ChannelFuture cfuture=channel.writeAndFlush(response);
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
					futrues.remove(String.valueOf(response.getSno()));
			}
		} else {
			return null;
		}
	}
	
	/**
	 * 发送无返回值
	 * @param response
	 */
	public void sendNoReturn(PullResponse response) {
		if (channel == null)
			channel = getChannel(inetAddr.toString());
		if (channel != null) {	
			channel.writeAndFlush(response);
		}
	}
	
	public void close() {
		if(channel!=null)
			try {
				channel.close().sync();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}

	public boolean isConnected() {
		return connected;
	}

	public boolean isClosed() {
		return (null == channel) || !channel.isOpen()
				|| !channel.isWritable() || !channel.isActive();
	}

	public boolean containsFuture(String key) {
		if(key==null)
			return false;
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
	
	public static void main(String[] args) {
		
		if(args == null || args.length < 6) {
			System.out.println("arguments number error!");
			System.exit(0);
		}
		String ip = "127.0.0.1";
		int port = 8088;
		int level = 2;
		for (int i = 0; i < args.length-1; i++) {
			if(args[i].equals("-i")) {
				ip = args[i+1];
			}
			if(args[i].equals("-p")) {
				port = Integer.valueOf(args[i+1]);
			}
			if(args[i].equals("-l")) {
				level = Integer.valueOf(args[i+1]);
			}
		}
		LoggerTool.LEVEL = level;
		PullAgent agent = new PullAgent(ip, port);
		agent.connect();
		
	}

}
