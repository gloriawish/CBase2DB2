package com.ecnu.sql;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TableMap {
	
	private static Map<String,String> tableMap = new HashMap<String,String>();
	
	private static Map<String,String> columnMap = new HashMap<String,String>();
	
	
	public static void main(String[] args) {
		load("tab.conf");
	}
	
	public static void load(String filePath) {
		try {
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = null;
			while( (line = br.readLine()) != null) {
				if(line.length() > 0) {
					line = line.toLowerCase();
					if(!line.contains("->"))
						throw new RuntimeException("格式不正确");
					String[] array = line.split("->");
					if(array.length == 1) {	//只能删除列
						if(array[0].contains(".")) {//列
							columnMap.put(array[0], null);
						} else {
							throw new RuntimeException("不能删除表");
						}
					} else if(array.length == 2) {
						if(array[0].contains(".")) {//列
							columnMap.put(array[0], array[1]);
						} else {//表
							tableMap.put(array[0], array[1]);
						}
					}
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO: handle exception
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public static boolean tableRename(String tableName) {
		if(tableName.contains(".")) {
			String[] array = tableName.split("\\.");
			return tableMap.containsKey(array[1]);
		}
		return tableMap.containsKey(tableName);
	}
	
	public static String getNewTableName(String oldTableName) {
		if(oldTableName.contains(".")) {
			String[] array = oldTableName.split("\\.");
			if(tableMap.containsKey(array[1])) {
				return array[0] + "." + tableMap.get(array[1]);
			} else {
				return null;
			}
		} else {
			if(tableMap.containsKey(oldTableName)) {
				return tableMap.get(oldTableName);
			} else {
				return null;
			}
		}
	}
	
	public static String getNewColumnName(String oldTableName,String oldColumnName) {
		
		if(oldTableName.contains(".")) {
			String[] array = oldTableName.split("\\.");
			oldTableName = array[1];
		}
		
		if(columnMap.containsKey(oldTableName + "." + oldColumnName)) {
			return columnMap.get(oldTableName + "." + oldColumnName);
		}
		return null;
	}
	
	
	public static boolean columnRename(String oldTableName, String columnName) {
		if(oldTableName.contains(".")) {
			String[] array = oldTableName.split("\\.");
			return columnMap.containsKey(array[1] + "." + columnName);
		} else {
			return columnMap.containsKey(oldTableName + "." + columnName);
		}
	}
	
	public static boolean isDeleteColumn(String oldTableName,String columnName) {
		if(oldTableName.contains(".")) {
			String[] array = oldTableName.split("\\.");
			oldTableName = array[1];
		}
		if(columnMap.containsKey(oldTableName + "." + columnName)) {
			return columnMap.get(oldTableName + "." + columnName) == null;
		}
		return false;
	}
	
	
	
}
