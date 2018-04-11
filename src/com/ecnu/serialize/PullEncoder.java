package com.ecnu.serialize;

import java.util.List;

import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

public class PullEncoder extends MessageToMessageEncoder<Object> {

	private Class<?> genericClass;
    public PullEncoder(Class<?> genericClass) {
        this.genericClass = genericClass;
    }
	@Override
	protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {

		ByteBuf sendBuf = Unpooled.buffer();
		
		if(genericClass.equals(PullRequest.class)) {
			PullRequest request = (PullRequest)msg;
			byte[] body = request.toBytes();
			if(body != null) {
				sendBuf.writeInt(body.length);
				sendBuf.writeBytes(body);
				out.add(sendBuf);
			}
        } else if(genericClass.equals(PullResponse.class)) {
        	PullResponse response = (PullResponse)msg;
			byte[] body = response.toBytes();
			if(body != null) {
				sendBuf.writeInt(body.length);
				sendBuf.writeBytes(body);
				out.add(sendBuf);
			}
        }
		
	}

}
