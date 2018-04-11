package com.ecnu.log;

import java.io.File;
import java.io.RandomAccessFile;

import com.ecnu.model.ReadResult;
import com.ecnu.tool.LoggerTool;

public class LogReader {
	
	
	public static long readLastLogSeq(String fileName, ReadResult rdResult) {
		int offset = 0;
		int readSize = 0;
		RandomAccessFile randomFile = null;
		try {
			File logFile = new File(fileName);
			randomFile = new RandomAccessFile(logFile, "r");
			long size = randomFile.length();
			while(true) {
				
				if(offset > size) {//偏移位置超过文件大小
					rdResult.code = -1;
					break;
				}
				
				//magic | header_length | version | header_checksum | reserved | data_length | data_zlength| data_checksum| seq | cmd
				int recordHeaderLength = 16 + 16 + 16 + 16 + 64 + 32 + 32 + 64 + 64 + 32;
				
				if (size - offset  < recordHeaderLength / 8) {//剩余长度小于头部长度
					rdResult.code = -2;
					break;
				}
				
				randomFile.seek(offset);//偏移到初始位置
				
				short magic = randomFile.readShort();//magic
				
				if (magic != (short)0xAAAAL) {	//魔数不正确
					LoggerTool.debug("magic code error" , new Throwable().getStackTrace());
					rdResult.code = -3;
					break;
				}
				
				int headLength = randomFile.readShort();//header_length
				
				LoggerTool.debug("header_length=" + headLength , new Throwable().getStackTrace());
				randomFile.readShort();//version
				randomFile.readShort();//header_checksum
				randomFile.readLong();//reserved
				
				int dataLength = randomFile.readInt();//data_length
				
				LoggerTool.debug("dataLength=" + dataLength,new Throwable().getStackTrace());
				
				int datazLength = randomFile.readInt();//data_zlength
				
				LoggerTool.debug("datazLength=" + datazLength, new Throwable().getStackTrace());
				
				randomFile.readLong();//data_checksum
				
				long seq = randomFile.readLong();//seq
				
				LoggerTool.debug("seq=" + seq, new Throwable().getStackTrace());
				
				int cmd = randomFile.readInt();//cmd
				
				LoggerTool.debug("cmd=" + cmd, new Throwable().getStackTrace());
				
				int logDataLength = dataLength - 8 - 4;//data_length - sizeof(seq) -sizeof(cmd)
				
				LoggerTool.debug("logDataLength=" + logDataLength, new Throwable().getStackTrace());
				
				int logEntryLength = recordHeaderLength / 8 + logDataLength;
				
				LoggerTool.debug("logEntryLength=" + logEntryLength, new Throwable().getStackTrace());
				
				if(size - offset  < logEntryLength * 8) {//日志内容不完整
					rdResult.code = -4;
					break;
				}
				
				randomFile.seek(offset);//偏移到初始位置
				readSize += logEntryLength;
				offset += logEntryLength;
				
				randomFile.seek(offset);//偏移到下一条日志
				
				LoggerTool.debug("", new Throwable().getStackTrace());
				if(cmd == 101) {//日志切换命令,就返回
					LoggerTool.info("switch log", new Throwable().getStackTrace());
					rdResult.code = 101;
					rdResult.readSize = readSize;
					return seq;
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
			rdResult.code = -1001;
		} finally {
			try {
				randomFile.close();
			} catch (Exception ex) {
				// TODO: handle exception
			}
		}
		rdResult.readSize = readSize;
		return -1;
	}
	
}
