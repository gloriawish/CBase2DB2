package com.ecnu.serialize;

import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RpcEncoder extends MessageToByteEncoder<Object>{
	
	private Class<?> genericClass;
    public RpcEncoder(Class<?> genericClass) {
        this.genericClass = genericClass;
    }
    /**
     *  | length:4 | body:n |
     */
	@Override
	protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out)
			throws Exception {
		
		if(genericClass.equals(PullRequest.class)) {
			PullRequest request = (PullRequest)msg;
			byte[] body = request.toBytes();
			if(body != null) {
				out.writeInt(body.length);
				out.writeBytes(body);
			}
        } else if(genericClass.equals(PullResponse.class)) {
        	PullResponse response = (PullResponse)msg;
			byte[] body = response.toBytes();
			if(body != null) {
				out.writeInt(body.length);
				out.writeBytes(body);
			}
        }
	}


}
