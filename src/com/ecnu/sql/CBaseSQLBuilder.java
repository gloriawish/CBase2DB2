package com.ecnu.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import com.ecnu.tool.TranstationSQL;

/**
 * CBase的SQL生成
 * @author Administrator
 *
 */
public class CBaseSQLBuilder  implements ISQLBuilder{

	//创表的语句
	private static Stack<String> CREATEBUCKET = new Stack<String>();
	private static List<Map<String,String>> ALLCOLUMN = new ArrayList<Map<String,String>>();
	private static String TABLENAME = "'NONE'";
	private static String DBNAME = "";
	@Override
	public List<String> buildDMLStatement(List<ObCellInfo> finalOp) {
		
		LinkedHashMap<String,StringBuilder> clob = new LinkedHashMap<String,StringBuilder>();
		LinkedHashMap<String,Boolean> clobSet = new LinkedHashMap<String,Boolean>();
		
		ObCellInfo baseCell = finalOp.get(0);
		
		TranstationSQL delTran = new TranstationSQL();
		TranstationSQL tran = new TranstationSQL();
		
		//获取表名
		String dbName = "NONE";
		String tableName = baseCell.getTable();
		String tableNameWithoutDb = "";
		if(baseCell.getTable().contains(".")) {
			String str = baseCell.getTable();
			String arr[] = str.split("\\.");
			dbName = arr[0];
			tableName = arr[1];
			tableNameWithoutDb = arr[1];
		} else {
			tableNameWithoutDb = tableName;
		}
		List<String> result = new ArrayList<String>();//最多两条
		int index = 0;
		
		//检查是否有delete操作，在最终序列表里会保留删除操作
		boolean isDel = false;
		for (index = 0; index < finalOp.size(); index++) {
			if(finalOp.get(index).getOpType().equals("DEL_ROW")) {
				isDel = true;
				continue;
			} else {
				break;
			}
		}
		
		StringBuilder rowKeyStr = new StringBuilder();
		for (int i = 0; i < baseCell.getRowkeyName().size(); i++) {
			
			rowKeyStr.append(baseCell.getRowkeyName().get(i));
			rowKeyStr.append(":");
			try {
				rowKeyStr.append(baseCell.getRowkey().get(i));
				if(i != baseCell.getRowkeyName().size() - 1) {
					rowKeyStr.append(",");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if(isDel) {
			String deleteBase = "delete from %s where %s";
			String selectBase = "select * from %s where %s";
			StringBuilder conditionStr = new StringBuilder();
			if(baseCell.getRowkeyName().size() > 0) {
				for (int i = 0; i < baseCell.getRowkeyName().size(); i++) {
					String name = baseCell.getRowkeyName().get(i); 
					String value = baseCell.getRowkey().get(i);
					conditionStr.append(name);
					conditionStr.append("=");
					conditionStr.append(value);
					if (i != baseCell.getRowkeyName().size() -1) {
						conditionStr.append(" and ");
					}
				}
				String delSql = String.format(deleteBase, tableName, conditionStr.toString());
				String selectSql = String.format(selectBase, tableName, conditionStr.toString());
				delTran.setType("delete");
				delTran.setSelect(selectSql);
				delTran.setDelete(delSql);
				delTran.setDb(dbName);
				delTran.setTable(tableNameWithoutDb);
				delTran.setPk(rowKeyStr.toString());
			} else {
				delTran.setType("delete");
				delTran.setSelect("empty");
				delTran.setDelete("empty");
				delTran.setDb(dbName);
				delTran.setTable(tableNameWithoutDb);
				delTran.setPk(rowKeyStr.toString());
			}
			
			if(!BlankList.inBlank(tableNameWithoutDb)) {
				result.add(delTran.toString());
			}
		}
		
		//有主键之外的其他字段
		if(index < finalOp.size()) {

			String updateBase = "update %s set %s where %s";
			
			String insertBase = "insert into %s(%s) values(%s)";
			
			String selectBase = "select * from %s where %s";
			
			StringBuilder columns = new StringBuilder();
			StringBuilder value = new StringBuilder();
			
			StringBuilder setStr = new StringBuilder();
			
			StringBuilder whereStr = new StringBuilder();
			
			if(baseCell.getRowkeyName().size() == baseCell.getRowkey().size())
			{
				for (int i = 0; i < baseCell.getRowkeyName().size(); i++) {
					String rowKeyName = baseCell.getRowkeyName().get(i); 
					String rowKeyValue = baseCell.getRowkey().get(i);
					columns.append(rowKeyName);
					value.append(rowKeyValue);
					whereStr.append(rowKeyName);
					whereStr.append("=");
					whereStr.append(rowKeyValue);
					if (i != baseCell.getRowkeyName().size() -1) {
						columns.append(",");
						value.append(",");
						whereStr.append(" and ");
					}
				}
				
				if(baseCell.getRowkeyName().size() > 0) {
					columns.append(",");
					value.append(",");
				}
			}
			
			for (; index < finalOp.size(); index++) {
				String key = finalOp.get(index).getColumn(); 
				String val = finalOp.get(index).getValue();
				
				//如果是clob字段
				if(SqlGenerate.isClob(key)) {
					if(clob.containsKey(getClobName(key)))
						if(!val.equals("null") && !val.equals("''")) {
							clob.get(getClobName(key)).append(clearName(val));
						}
					if(!clob.containsKey(getClobName(key))) {
						if(!val.equals("null") && !val.equals("''"))
							clob.put(getClobName(key), new StringBuilder("#" + clearName(val)));
						else {
							if(val.equals("null")) {
								clob.put(getClobName(key), new StringBuilder("null"));
							}
							if(val.equals("''")) {
								clob.put(getClobName(key), new StringBuilder("#"));
							}
						}
					}
					
					if(!clobSet.containsKey(getClobName(key))) {
						value.append("?");
						columns.append(getClobName(key));
						setStr.append(getClobName(key));
						setStr.append("=");
						setStr.append("?");
						if (index != finalOp.size() -1) {
							columns.append(",");
							value.append(",");
							setStr.append(",");
						}
						clobSet.put(getClobName(key), true);
					}
					
				} else {
					value.append(val);
					columns.append(key);
					setStr.append(key);
					setStr.append("=");
					setStr.append(val);
					if (index != finalOp.size() -1) {
						columns.append(",");
						value.append(",");
						setStr.append(",");
					}
				}
			}
			if(columns.toString().endsWith(",")) {
				columns.deleteCharAt(columns.length() - 1);
			}
			if(setStr.toString().endsWith(",")) {
				setStr.deleteCharAt(setStr.length() - 1);
			}
			if(value.toString().endsWith(",")) {
				value.deleteCharAt(value.length() - 1);
			}
			String updateSql = String.format(updateBase, tableName, setStr.toString(), whereStr.toString());
			String insertSql = String.format(insertBase, tableName, columns.toString(), value.toString());
			String selectSql = String.format(selectBase, tableName, whereStr.toString());
			
			tran.setType("insertupdate");
			tran.setSelect(selectSql);
			tran.setInsert(insertSql);
			tran.setUpdate(updateSql);
			tran.setDb(dbName);
			tran.setTable(tableNameWithoutDb);
			tran.setPk(rowKeyStr.toString());
			List<String> clobList = new ArrayList<String>();
			for (Entry<String, StringBuilder> item : clob.entrySet()) {
				clobList.add(item.getValue().toString());
			}
			tran.setClob(clobList);
			
			//是否在黑名单中
			if(!BlankList.inBlank(tableNameWithoutDb)) {
				result.add(tran.toString());
			}
		}
		return result;
		
	}
	
	
	
	public String getClobName(String column) {
		
		String real = column.substring(0, column.lastIndexOf("_"));
		return real;
		
	}

	@Override
	public List<String> buildDDLStatement(List<ObCellInfo> finalOp) {
		ObCellInfo baseCell = finalOp.get(0);
		String tableName = baseCell.getTable();
		String tableNameWithoutDb = "";
		String dbName = "";
		if(baseCell.getTable().contains(".")) {
			String str = baseCell.getTable();
			String arr[] = str.split("\\.");
			dbName = arr[0];
			tableNameWithoutDb = arr[1];
		} else {
			tableNameWithoutDb = tableName;
		}
		
		int index = 0;
		
		//检查是否有delete操作，在最终序列表里会保留删除操作
		boolean isDel = false;
		for (index = 0; index < finalOp.size(); index++) {
			if(finalOp.get(index).getOpType().equals("DEL_ROW")) {
				isDel = true;
				continue;
			} else {
				break;
			}
		}
		List<String> result = new ArrayList<String>();
		
		//删除语句
		if(isDel && BlankList.inBlank(tableNameWithoutDb)) {
			int tmpPos = 0;
			if(-1 != (tmpPos = BlankList.inDeleteTable(tableNameWithoutDb))) {
				if(tmpPos == 1) {
					String delTableName = "";
					for (int i = 0; i < baseCell.getRowkeyName().size(); i++) {
						String rowKeyName = baseCell.getRowkeyName().get(i); 
						String rowKeyValue = baseCell.getRowkey().get(i);
						if(rowKeyName.equals("table_name")) {
							delTableName = rowKeyValue;
						}
					}
					result.add(gengerateDropStatement(delTableName,dbName));
				}
			}
			
			String dropDb = tryBuildDropDatabase(finalOp,tableNameWithoutDb);
			if(dropDb != null) {
				result.add(dropDb);
			}
			
			
		}
		
		//是否在黑名单中
		if(!isDel && BlankList.inBlank(tableNameWithoutDb)) {
			//识别各种顺序的操作
			//1、create table
			String create = tryBuildCreateStatement(finalOp,tableNameWithoutDb);
			if(create != null) {
				result.add(create);
			}
			
			//2、create database
			String createDb = tryBuildCreateDatabase(finalOp,tableNameWithoutDb);
			if(createDb != null) {
				result.add(createDb);
			}
			//3、alter table
			//4、...
			
			
		}
		
		return result;
	}

	public static String tryBuildDropDatabase(List<ObCellInfo> finalOp,String tableNameWithoutDb) {
		ObCellInfo baseCell = finalOp.get(0);
		//删除数据库
		if(BlankList.isDatabase(tableNameWithoutDb)) {
			TranstationSQL dropTran = new TranstationSQL();
			dropTran.setType("dropdatabase");
			dropTran.setDb("");
			String delDbName = "";
			for (int i = 0; i < baseCell.getRowkeyName().size(); i++) {
				String rowKeyName = baseCell.getRowkeyName().get(i); 
				String rowKeyValue = baseCell.getRowkey().get(i);
				if(rowKeyName.equals("db_name")) {
					delDbName = rowKeyValue;
				}
			}
			dropTran.setDdl(String.format("drop database %s", clearName(delDbName)));
			return dropTran.toString();
		}
		return null;
	}
	
	public static String tryBuildCreateDatabase(List<ObCellInfo> finalOp,String tableNameWithoutDb) {
		
		String dbName = null;
		if(BlankList.isDatabase(tableNameWithoutDb)) {
			TranstationSQL createTran = new TranstationSQL();
			createTran.setType("createdatabase");
			createTran.setDb("");
			for (int i = 0; i < finalOp.get(0).getRowkeyName().size(); i++) {
				String rowKeyName = finalOp.get(0).getRowkeyName().get(i); 
				String rowKeyValue = finalOp.get(0).getRowkey().get(i);
				if(rowKeyName.equals("db_name")) {
					dbName = rowKeyValue;
				}
			}
			
			createTran.setDdl(String.format("create database %s", clearName(dbName)));
			
			return createTran.toString();
		}
		return null;
	}
	
	public static String tryBuildCreateStatement(List<ObCellInfo> finalOp,String tableNameWithoutDb) {
		int pos = 0;
		//是否是创建表的语句
		if(-1 != (pos = BlankList.inCreateTable(tableNameWithoutDb))) {
			if(CREATEBUCKET.size() > 0) {
				//顺序出错的时候
				if(Integer.valueOf(CREATEBUCKET.peek()) > pos) {
					resetCreat();
				}
			} else {
				if(pos != 1) {//一言不合就清空数据
					resetCreat();
				}
			}
			if(pos == 3) {
				//找到表名
				for (int i = 0; i < finalOp.get(0).getRowkeyName().size(); i++) {
					String rowKeyName = finalOp.get(0).getRowkeyName().get(i); 
					String rowKeyValue = finalOp.get(0).getRowkey().get(i);
					if(rowKeyName.equals("table_name")) {
						TABLENAME = rowKeyValue;
					}
					if(rowKeyName.equals("db_name")) {
						DBNAME = rowKeyValue;
					}
				}
				CREATEBUCKET.add(String.valueOf(pos));
			} else if(pos == 4) {
				//添加列名
				//找到表名
				Map<String,String> colDef = new HashMap<String,String>();
				for (int i = 0; i < finalOp.get(0).getRowkeyName().size(); i++) {
					String rowKeyName = finalOp.get(0).getRowkeyName().get(i); 
					String rowKeyValue = finalOp.get(0).getRowkey().get(i);
					colDef.put(rowKeyName, rowKeyValue);
				}
				for (int i = 0; i < finalOp.size(); i++) {
					String key = finalOp.get(i).getColumn(); 
					String val = finalOp.get(i).getValue();
					colDef.put(key, val);
				}
				ALLCOLUMN.add(colDef);
				if(Integer.valueOf(CREATEBUCKET.peek()) < 4)
					CREATEBUCKET.add(String.valueOf(pos));
			} else {
				CREATEBUCKET.add(String.valueOf(pos));
			}
			if(CREATEBUCKET.size() == 6) {
				TranstationSQL createTran = new TranstationSQL();
				createTran.setType("create");
				createTran.setDb(DBNAME);
				createTran.setDdl(generateCreateStatement());
				return createTran.toString();
			}
		} else {
			resetCreat();
		}
		return null;
	}
	
	private static void resetCreat() {
		CREATEBUCKET.clear();
		TABLENAME = "'NONE'";
		ALLCOLUMN.clear();
	}
	
	public static String generateCreateStatement() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < ALLCOLUMN.size(); i++) {
			sb.append(buildColumnDefine(ALLCOLUMN.get(i)));
			if(i != ALLCOLUMN.size() -1) {
				sb.append(",");
			} else {
				sb.append(",");
				sb.append(buildPrimaryKey());
			}
		}
		String name = clearName(TABLENAME);
		String base = String.format("create table %s(%s)",name,sb.toString());
		resetCreat();
		return base;
	}
	
	private static String buildPrimaryKey() {
		StringBuilder pk = new StringBuilder();
		
		pk.append("primary key(");
		int count = 0;
		for (int i = 0; i < ALLCOLUMN.size(); i++) {
			if(!ALLCOLUMN.get(i).get("rowkey_id").equals("0")) {
				count++;
			}
		}
		
		for (int i = 1; i <= count; i++) {
			for (int j = 0; j < ALLCOLUMN.size(); j++) {
				if(ALLCOLUMN.get(j).get("rowkey_id").equals(String.valueOf(i))) {
					pk.append(clearName(ALLCOLUMN.get(j).get("column_name")));
				}
			}
			
			if(i != count) {
				pk.append(",");
			}
		}
		pk.append(")");
	
		return pk.toString();
		
	}
	
	private static String buildColumnDefine(Map<String,String> colDef) {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(clearName(colDef.get("column_name")));
		sb.append(" ");
		ObObjType type = ObObjType.valueOf(Integer.valueOf(colDef.get("data_type")));
		switch (type) {
		case ObBoolType:
			sb.append("bool");
			break;
		case ObDateType:
			sb.append("date");
			break;
		case ObPreciseDateTimeType://datetime or  timestamp
			sb.append("timestamp");
			break;
		case ObDecimalType:
			sb.append("decimal");
			sb.append("(" + colDef.get("data_precision") + ",");
			sb.append(colDef.get("data_scale") + ")");
			break;
		case ObDoubleType:
			sb.append("double");
			break;
		case ObFloatType:
			sb.append("float");
			break;
		case ObInt32Type:
			sb.append("int");
			break;
		case ObIntType:
			sb.append("bigint");
			break;
		case ObVarcharType:
			sb.append("varchar");
			if(Integer.valueOf(colDef.get("data_length")) != -1) {
				sb.append("(" + colDef.get("data_length") + ")");
			}
			break;
		case ObTimeType:
			sb.append("time");
		case ObCreateTimeType:
			sb.append("createtime");
			break;
		case ObModifyTimeType:
			sb.append("modifytime");
			break;
		default:
			sb.append("unkown type:" + type.getValue());
			break;
		
		}
		if(Integer.valueOf(colDef.get("nullable")) == 0) {
			sb.append(" ");
			sb.append("not null");
		}
		return sb.toString();
	}
	
	private static String clearName(String str) {
		String name = str.substring(1, str.length() - 1);
		return name;
	}
	
	public static String gengerateDropStatement(String tableName,String dbName) {
		String name = tableName.substring(1, tableName.length() - 1);
		String base = String.format("drop table %s", name);
		TranstationSQL dropTran = new TranstationSQL();
		dropTran.setType("drop");
		dropTran.setDdl(base);
		dropTran.setDb(dbName);
		return dropTran.toString();
	}
}
