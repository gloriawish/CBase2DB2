package com.ecnu.tool;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ecnu.model.BytesWrapper;
public class BytesCachePool {
	private BytesCachePool() {
		
	}
	private  BlockingQueue<BytesWrapper> queue = new ArrayBlockingQueue<BytesWrapper>(1024);
	
	private ReadWriteLock rwLock = new ReentrantReadWriteLock();
	/**
	 * BytesWrapper入队
	 * @param item
	 */
	public void push(BytesWrapper item) {
		rwLock.writeLock().lock();
		queue.add(item);
		rwLock.writeLock().unlock();
	}
	
	/**
	 * BytesWrapper出队
	 * @return
	 */
	public BytesWrapper pop() {
		try {
			return queue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			return null;
		}
	}
	
	
	public int getCount() {
		return queue.size();
	}
	
	public static BytesCachePool getInstance() {
		return InnerHolder.INSTANCE;
	}
	private static class InnerHolder {
		public static final BytesCachePool INSTANCE =  new BytesCachePool();
	}

}
