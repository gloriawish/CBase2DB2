package com.ecnu.netty.listener;

import com.ecnu.netty.model.PullResponse;

import io.netty.channel.Channel;

public abstract class ResponseListener {

	public void onBaseInfoReceived(PullResponse msg, Channel channel) {
		
	}
}
