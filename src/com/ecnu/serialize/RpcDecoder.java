package com.ecnu.serialize;

import java.util.List;

import com.ecnu.netty.model.PullRequest;
import com.ecnu.netty.model.PullResponse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class RpcDecoder extends ByteToMessageDecoder{
	
	private Class<?> genericClass;
    public RpcDecoder(Class<?> genericClass) {
        this.genericClass = genericClass;
    }
	@Override
	public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		
		int HEAD_LENGTH=4;
        if (in.readableBytes() < HEAD_LENGTH) {
            return;
        }
        in.markReaderIndex();               
        int dataLength = in.readInt();       
        if (dataLength < 0) { 				 
            ctx.close();
        }
        if (in.readableBytes() < dataLength) { 
            in.resetReaderIndex();
            return;
        }
        int bodyLength=dataLength;
    	byte[] body = new byte[bodyLength]; 
    	in.readBytes(body);
        if (genericClass.equals(PullRequest.class)) {
        	PullRequest request = PullRequest.deserialize(body);
        	out.add(request);
        } else if (genericClass.equals(PullResponse.class)) {
        	PullResponse response = PullResponse.deserialize(body);
        	out.add(response);
        }
	}
}
