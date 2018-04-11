package com.ecnu.sql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ecnu.tool.LoggerTool;

public class SqlFileWriter {
	
	private String savePath;
	
    private int fileMaxLength = 64 * 1024 * 1024;		// 40MB = 40*1024*1024
    
    private int fileId;
    
    private BufferedWriter bw;
    
    private String currentFile;

	private RandomAccessFile fs;
	
	private Lock lock = new ReentrantLock();
    
    public SqlFileWriter(String savepath) {
		this.savePath = savepath;
		//计算最大ID
		File root = new File(savepath);
		File[] files = root.listFiles();
		int maxId = 0;
		for (int i = 0; i < files.length; i++) {
			String id = files[i].getName().split("\\.")[0];
			if(maxId < Integer.parseInt(id)) {
				maxId = Integer.parseInt(id);
			}
		}
		fileId = maxId;
		currentFile = getNextFileName();
		open(currentFile);
		
	}
    
	private void open(String filePath) {
		try {
			bw = new BufferedWriter(new FileWriter(new File(filePath), true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public String getCurrentFile() {
		return currentFile;
	}
	
	private String getNextFileName() {
		fileId++;
		String fileName = String.format("%s/%s.sql", savePath, fileId);
		return fileName;
	}
	
	public void beginWrite() {
		lock.lock();
	}
	
	public void endWrite() {
		lock.unlock();
	}
	
	public boolean writeSingleSql(String sql) {
		try {
			bw.write(sql);
			bw.newLine();
			bw.flush();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return false;
	}
	
	public void checkNeedSwitchNextFile() {
		if(!sizeCheck()) {
			switchNextFile();
		}
	}
	
	public  String switchNewFile() {
		switchNextFile();
		return currentFile;
	}
	
	private boolean switchNextFile() {
		lock.lock();
		currentFile = getNextFileName();
		try {
			close();
			open(currentFile);
			LoggerTool.info("switch next sql file:" + currentFile, new Throwable().getStackTrace());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
		return false;
	}
	
	private boolean sizeCheck() {
		try {
			fs = new RandomAccessFile(currentFile, "r");
			if(fs.length() < fileMaxLength) {
				fs.close();
				return true;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public void close() {
		try {
			bw.flush();
			bw.close();
			bw = null;
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	
	

}
