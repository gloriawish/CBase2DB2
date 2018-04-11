package com.ecnu.model;

public class PullConstant {

	public static String EMPTYLOG = "";
	public static int PULL_LOG_REQUEST = 1000;		//拉取日志的请求类型		Server->Agent
	public static int LOGIN_AUTH = 1001;			//agent注册登录的类型	Agent->Server
	public static int HEART_BEAT = 1002;			//心跳信息				Agent->Server
	
	public static int BASE_INFO_REQUEST = 1003;		//拉取基本信息			Server->Agent
	
	public static int SCHEMA_REPORT = 1004;			//schema信息			Agent->Server
	
	public static int SCHEMA_REQUIRE = 1005;		//					Agent->Server
	
	public static int SCHEMA_APPEND = 1006;			//追加Schema			Server->Agent
	
	public static int PULL_LOG_CRC_REQUEST = 1007;  //					Server->Agent
	
	public static int PULL_LOG_RESPONSE = 2001;		//拉取日志的响应类型		Agent->Server
	
	public static int BASE_INFO_RESPONSE = 2002;
	
	public static int LOGIN_AUTH_RESULT = 2003;		//					Server->Agent
	
	public static int SCHEMA_RESULT = 2004;			//下发schema			Server->Agent
	
	public static int PULL_LOG_CRC_RESPONSE = 2005;	//					Agent->Server
	
	
	public static int CONTROL_SET_DELAY = 3001;
	public static int CONTROL_SET_COUNT = 3002;
	public static int CONTROL_SET_LEVEL = 3003;
	
	public static int CONTROL_RESULT = 3004;
	
	public static int CONTROL_STAT_REPORT = 3005;
	
	public static int CONTROL_SHOW_LOG = 3006;
	
	public static int CONTROL_LOG = 3007;
	
	public static int CONTROL_DUMP_SCHEMA = 3008;
	
	public static int CONTROL_STOP = 3009;
	
	public static int CONTROL_GOON = 3010;
	
	public static int CONTROL_KILL = 3011;
	
	public static int CONTROL_SWITCH = 3012;
	
	public static int CONTROL_CLEINT = 888999;
	
	
	public static String SCHEMA_CACHE_UID = "SCHEMA_88888888888888";
	
	
	public static int OB_LOG_SWITCH_LOG = 101;
	
	public static int OB_LOG_CHECKPOINT = 102;
	
	public static int OB_LOG_NOP = 103;
	
	public static int OB_LOG_UPS_MUTATOR = 200;
	public static int OB_UPS_SWITCH_SCHEMA = 201;
	public static int OB_UPS_SWITCH_SCHEMA_MUTATOR = 202;
	public static int OB_UPS_SWITCH_SCHEMA_NEXT = 203;
	public static int OB_UPS_WRITE_SCHEMA_NEXT = 204;
	
	public static int LOG_END = -1;
	
	public static int SEQ_INVALID = -2;//读取到的日志无效
	
	public static int SCHEMA_SWITCH = -3;
	
	public static int SCHEMA_INVAILD = -4;//schema无效
	
	public static int AGENT_ERROR = -5;//schema无效
	
	public static int BUFFER_OVERFLOW = -6;
	
	public static int NORMAL = 0;
}
