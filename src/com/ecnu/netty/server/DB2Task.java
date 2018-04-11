package com.ecnu.netty.server;


import com.ecnu.model.BytesWrapper;
import com.ecnu.tool.BytesCachePool;
import com.ecnu.tool.LoggerTool;

public class DB2Task implements Runnable{

	@Override
	public void run() {
		
		while(true) {
			//从byte的cache队列拉取数据进行反序列化为SQL执行
			BytesWrapper logBytes = BytesCachePool.getInstance().pop();
			if(logBytes != null) {
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				LoggerTool.info("empty queue", new Throwable().getStackTrace());
			}
			
		}
		
	}

}
