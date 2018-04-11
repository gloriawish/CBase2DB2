package com.ecnu.model;

import java.util.ArrayList;
import java.util.List;

import com.ecnu.netty.model.TaskConf;

public class CDCConfig {

	private String ip;
	private int port;
	private int delay;
	private int logLevel;
	private int count;
	private String rootServer;
	private String rootServerPort;
	private String userName;
	private String userPass;
	private String seq;
	
	private String savePath;
	
	private String upsLog;
	
	private String schemaPath;
	
	private boolean restart;
	
	private List<String> upsList;
	
	private String master;
	
	private String dbType;

	private String filter;
	
	public CDCConfig() {
		upsList = new ArrayList<String>();
	}
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public int getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(int logLevel) {
		this.logLevel = logLevel;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getRootServer() {
		return rootServer;
	}

	public void setRootServer(String rootServer) {
		this.rootServer = rootServer;
	}

	public String getRootServerPort() {
		return rootServerPort;
	}

	public void setRootServerPort(String rootServerPort) {
		this.rootServerPort = rootServerPort;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserPass() {
		return userPass;
	}

	public void setUserPass(String userPass) {
		this.userPass = userPass;
	}

	public String getSeq() {
		return seq;
	}

	public void setSeq(String seq) {
		this.seq = seq;
	}

	public String getUpsLog() {
		return upsLog;
	}

	public void setUpsLog(String upsLog) {
		this.upsLog = upsLog;
	}

	public String getSchemaPath() {
		return schemaPath;
	}

	public void setSchemaPath(String schemaPath) {
		this.schemaPath = schemaPath;
	}

	public boolean isRestart() {
		return restart;
	}

	public void setRestart(boolean restart) {
		this.restart = restart;
	}

	public List<String> getUpsList() {
		return upsList;
	}

	public void setUpsList(List<String> upsList) {
		this.upsList = upsList;
	}

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}
	
	
	public String getSavePath() {
		return savePath;
	}

	public void setSavePath(String savePath) {
		this.savePath = savePath;
	}
	

	public String getMaster() {
		return master;
	}

	public void setMaster(String master) {
		this.master = master;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public TaskConf getTaskConf() {
		TaskConf conf = new TaskConf();
		conf.setLastSeq(Long.valueOf(seq));
		conf.setLogName(Integer.valueOf(upsLog));
		return conf;
	}
	
	
}
