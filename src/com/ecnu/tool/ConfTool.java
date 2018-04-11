package com.ecnu.tool;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import com.ecnu.model.CDCConfig;
import com.ecnu.netty.server.PullServer;

public class ConfTool {

	
	public static CDCConfig loadConf(String configfile) {
		CDCConfig conf = new CDCConfig();
		try {
			InputStream inputStream = new BufferedInputStream(new FileInputStream(configfile));
	        Properties prop = new Properties();
	        prop.load(inputStream);
	        
	        //加载ups列表
	        String[] array = prop.getProperty("ups").trim().split(",");
	        for (String string : array) {
	        	conf.getUpsList().add(string.trim());
			}
	        
	        //其他参数
	        conf.setIp(prop.getProperty("host","127.0.0.1").trim());
	        conf.setPort(Integer.valueOf(prop.getProperty("port","8089").trim()));
	        conf.setDelay(Integer.valueOf(prop.getProperty("delay","1000").trim()));
	        conf.setLogLevel(Integer.valueOf(prop.getProperty("level","3").trim()));
	        conf.setCount(Integer.valueOf(prop.getProperty("count","10").trim()));
	        
	        conf.setRootServer(prop.getProperty("rs_ip","127.0.0.1").trim());
	        conf.setRootServerPort(prop.getProperty("rs_port","2500").trim());
	        
	        conf.setUserName(prop.getProperty("user_name","admin").trim());
	        conf.setUserPass(prop.getProperty("user_pass","admin").trim());
	        
	        conf.setSchemaPath(prop.getProperty("schema_path","null").trim());
	        conf.setSavePath(prop.getProperty("save_path","./sqlfile").trim());
	        conf.setSeq(prop.getProperty("start_seq","0").trim());
	        
	        conf.setUpsLog(prop.getProperty("start_log","1").trim());
	        
	        conf.setDbType(prop.getProperty("db_type","OceanBase").trim());
	        
	        conf.setMaster(prop.getProperty("master","127.0.0.1").trim());
	        
	        conf.setFilter(prop.getProperty("filter",null));
	        
	        String debug = prop.getProperty("debug","false");
	        if(debug.equals("true"))
	        	PullServer.DEBUG = true;
	        else
	        	PullServer.DEBUG = false;
	        if (prop.getProperty("restart","false").trim().equals("true")) {
				conf.setRestart(true);
			} else {
				conf.setRestart(false);
			}
	        return conf;
	        
		} catch (Exception e) {
			// TODO: handle exception
		}
		return null;
		
	}
	
	
}
