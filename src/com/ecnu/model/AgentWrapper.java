package com.ecnu.model;

import io.netty.channel.Channel;

public class AgentWrapper {
	private Channel channel;
	private AgentStatus status;
	private boolean isMaster;
	
	public AgentWrapper(Channel ch) {
		this.channel = ch;
		status = AgentStatus.Disable;
		isMaster = false;
	}
	
	public boolean isAvailable() {
		return status != AgentStatus.Disable && status != AgentStatus.OffLine;
	}
	
	public Channel getChannel() {
		return channel;
	}
	public void setChannel(Channel channel) {
		this.channel = channel;
	}
	public AgentStatus getStatus() {
		return status;
	}
	public void setStatus(AgentStatus status) {
		this.status = status;
	}
	public boolean isMaster() {
		return isMaster;
	}
	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}
	
}

