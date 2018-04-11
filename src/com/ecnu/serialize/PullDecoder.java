package com.ecnu.serialize;

import java.util.List;

import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

public class PullDecoder extends MessageToMessageDecoder<Object>{

	private Class<?> genericClass;
    public PullDecoder(Class<?> genericClass) {
        this.genericClass = genericClass;
    }
	@Override
	protected void decode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
		ByteBuf readBuf = (ByteBuf)msg;
		int bodyLength=readBuf.readInt();
	    byte[] body = new byte[bodyLength];
	    readBuf.readBytes(body, 0, bodyLength);
		if (genericClass.equals(PullRequest.class)) {
			PullRequest request = PullRequest.deserialize(body);
			out.add(request);
	    } else if (genericClass.equals(PullResponse.class)) {
	    	PullResponse response = PullResponse.deserialize(body);
	    	out.add(response);
	    }
	}

}
