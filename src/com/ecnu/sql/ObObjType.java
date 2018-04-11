package com.ecnu.sql;

import java.util.HashMap;
import java.util.Map;

public enum ObObjType {
	  ObMinType(-1),
	  ObNullType(0),   // 空类型
	  ObIntType(1),
	  ObFloatType(2),              // @deprecated
	
	  ObDoubleType(3),             // @deprecated
	  ObDateTimeType(4),           // @deprecated
	  ObPreciseDateTimeType(5),    // =5
	
	  ObVarcharType(6),
	  ObSeqType(7),
	  ObCreateTimeType(8),
	
	  ObModifyTimeType(9),
	  ObExtendType(10),
	  ObBoolType(11),
	
	  ObDecimalType(12),            // aka numeric
	  //add peiouya [DATE_TIME] peiouya 20150831:e
	  ObDateType(13),
	  ObTimeType(14),
	  //this type only use for date and time calculate, is temp type. its measure unit is microsecond.
	  ObIntervalType(15),
	  //add 20150831:e
	  //add lijianqiang [INT32] 20150929:b
	  ObInt32Type(16),
	  //add 20150929:e
	  ObMaxType(17);
	  private int value;
	  ObObjType(int value) {
		  this.value = value;
	  }
	  
	  private static Map<Integer,ObObjType> map = new HashMap<Integer, ObObjType>();
	  static {
		  for (ObObjType item : values()) {
			  map.put(item.getValue(), item);
		  }
	  }
	  public int getValue() {
		  return value;
	  }
	  
	  public static ObObjType valueOf(int value) {
		  return map.get(value);
	  }
}
