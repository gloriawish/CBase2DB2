package com.ecnu.tool;


public class ByteTool {

	/**
	 * 
	 * @param buf
	 * @param len
	 * @param pos
	 * @return
	 */
	public static int byteToInt32(byte[] buf, int len, int pos) {
		if (buf != null && len - pos > 4) {
			int val = (buf[pos++] & 0xff) << 24;
			val |= (buf[pos++] & 0xff) << 16;
			val |= (buf[pos++] & 0xff) << 8;
			val |= (buf[pos++] & 0xff);
			return val;
		} else {
			//TODO
			throw new RuntimeException();
		}
		
//		ByteBuffer buffer = ByteBuffer.allocate(4);
//		buffer.put(buf, pos, 4);
//		buffer.flip();
//		return buffer.getInt();
		
//		ByteArrayInputStream bis = new ByteArrayInputStream(buf,pos,len - pos);
//		DataInputStream dis = new DataInputStream(bis);
//		try {
//			return dis.readInt();
//		} catch (IOException e) {
//			throw new RuntimeException();
//		}
	}

	/**
	 * 
	 * @param val
	 * @return
	 */
	public static byte[] int32ToByte(int val) {
		return new byte[] {(byte)((val >> 24) & 0xff),
				(byte)((val >> 16) & 0xff),
				(byte)((val >> 8) & 0xff),
				(byte)(val & 0xff)};
		
//		ByteBuffer buffer = ByteBuffer.allocate(4);
//		buffer.putInt(val);
//		return buffer.array();
		
//		ByteArrayOutputStream bos = new ByteArrayOutputStream();
//		DataOutputStream dos = new DataOutputStream(bos);
//		dos.writeInt(val);
//		bos.toByteArray();
	}
	/**
	 * 
	 * @param buf
	 * @param len
	 * @param pos
	 * @return
	 */
	public static long byteToInt64(byte[] buf, int len, int pos) {
		if (buf != null && len - pos > 4) {
			int val = (buf[pos++] & 0xff) << 56;
			val |= (buf[pos++] & 0xff) << 48;
			val |= (buf[pos++] & 0xff) << 40;
			val |= (buf[pos++] & 0xff) << 32;
			val |= (buf[pos++] & 0xff) << 24;
			val |= (buf[pos++] & 0xff) << 16;
			val |= (buf[pos++] & 0xff) << 8;
			val |= (buf[pos++] & 0xff);
			return val;
		} else {
			//TODO
			throw new RuntimeException();
		}
	}
	
	public static int getUnsignedByte(short data) {
		return data & 0x0FFFF;
	}
	public static long getUnsignedInt(int data) {
		return data & 0x0FFFFFFFF;
	}
	
	
	
	
}
