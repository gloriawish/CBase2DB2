package com.ecnu.sql;

import java.util.HashSet;
import java.util.Set;

public class WhileList {
	public static Set<String> WHITE = new HashSet<>();
	
	public static void loadFromConf(String path) {
		
	}
	
	public static boolean inWhite(String tableName) {
		if(WHITE.contains(tableName)) {
			return true;
		}
		return false;
	}
}
