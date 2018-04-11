package com.ecnu.sql;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ecnu.model.BytesWrapper;
import com.ecnu.netty.server.PullServer;
import com.ecnu.tool.LoggerTool;

/**
 * SQL构建器
 * @author Administrator
 *
 */
public class SqlGenerate {

	public static ISQLBuilder SQLBUILDER = null;
	
	public static List<String> byteToSql(List<BytesWrapper> data) {
		//生成mutator实体
		MutatorEntry entry = SqlGenerate.buildMutatorEntry(data);
		//生成一个事务的所有sql
		if(PullServer.DEBUG) {
			LoggerTool.info(entry.toString(), new Throwable().getStackTrace());
		}
		List<String> sqls = SqlGenerate.mutatorToSql(entry);//一个事务的数据
		return sqls;
	}
	
	private static List<String> mutatorToSql(MutatorEntry mutatorEntry) {
		List<String> result = new ArrayList<String>();
		//获取到每个mutator的最终操作
		Map<String, List<ObCellInfo>> finalOps = mutatorEntry.finalOption();
		//对每个字段的操作合并生成一条或两条sql
		for (Entry<String, List<ObCellInfo>> item : finalOps.entrySet()) {
			List<String> sql = mergeCellToSql(item.getValue());
			result.addAll(sql);
		}
		return result;
	}
	
	private static MutatorEntry buildMutatorEntry(List<BytesWrapper> data) {
		
		BytesWrapper bw =  data.get(0);
		MutatorEntry entry = new MutatorEntry();
		entry.setSeq(bw.getSeq());
		for (int i = 0; i < data.size(); i++) {
			BytesWrapper bytes =  data.get(i);
			String str = new String(bytes.getBytes());
			ObCellInfo cell = parseCell(str);
			if(cell != null)
				entry.addCell(cell);
		}
		return entry;
	}
	
	private static ObCellInfo parseCell(String data) {
		
		if(PullServer.DEBUG) {
			LoggerTool.info(data, new Throwable().getStackTrace());
		}
		String[] result = new String[6];
		
		result[0] = data.substring(0, data.indexOf("table->") - 1).replace("op_type->", "");
		
		result[1] = data.substring(data.indexOf("table->"), data.indexOf("column->") - 1).replace("table->", "");
		
		result[2] = data.substring(data.indexOf("column->"), data.indexOf("row_key->") - 1).replace("column->", "");
		
		result[3] = data.substring(data.indexOf("row_key->"), data.indexOf("row_key_info->") - 1).replace("row_key->", "");
		
		result[5] = data.substring(data.indexOf("row_key_info->"), data.indexOf("value->") - 1).replace("row_key_info->", "");
		
		result[4] = data.substring(data.indexOf("value->")).replace("value->", "");
		
		
		ObCellInfo cell = new ObCellInfo();
		cell.setOpType(result[0]);
		cell.setTable(result[1]);
		cell.setColumn(result[2]);
		String oldTable = cell.getTable();
		String oldColumn = cell.getColumn();
		//映射表名
		if(TableMap.tableRename(cell.getTable())) {
			cell.setTable(TableMap.getNewTableName(cell.getTable()));
		}
		if(TableMap.columnRename(oldTable,cell.getColumn())) {
			//需要移除这一列
			if(TableMap.isDeleteColumn(oldTable,cell.getColumn())) {
				return null;
			} else {
				cell.setColumn(TableMap.getNewColumnName(oldTable,cell.getColumn()));
			}
		}
		
		
		//主键值
		final char oo = 15;//linux ^O 符号
		String[] arr1 = result[3].split(String.valueOf(oo));
		
		for (int i = 0; i < arr1.length; i++) {
			String[] temp = arr1[i].split(":");
			if(temp.length == 1)
				cell.getRowkey().add("null");
			else {
				int len = new String(temp[0]+":").length();
				String value = arr1[i].substring(len);
				if(temp[0].equals("date")) {
					//cell.getRowkey().add(timeStampToDate(value));
					cell.getRowkey().add("'" + value + "'");
				} else if(temp[0].equals("time")) {
					//cell.getRowkey().add(timeStampToDate(value));
					cell.getRowkey().add("'" + value + "'");
				} else if(temp[0].equals("datetime")) {
					//cell.getRowkey().add(timeStampToDate(value));
					cell.getRowkey().add("'" + value + "'");
				} else if(temp[0].equals("precisedatetime")) {
					//cell.getRowkey().add(timeStampToDate(value));
					cell.getRowkey().add("'" + value + "'");
				} else if(temp[0].equals("varchar")) {
					
					//给单引号加上转义字符
					if(value.length()>2 && !isClob(oldColumn)) {
						String src = value.substring(1,value.length() - 1);
						src = src.replace("'", "''");//连续两个单引号
						value = "'" + src + "'";
					}
					
					cell.getRowkey().add(value);
				} 
				else {
					cell.getRowkey().add(value);
				}
			}
		}
		
		//列值
		String[] arr2 = result[4].split(":");
		if(arr2.length == 1)
			cell.setValue("null");
		else {
			int len = new String(arr2[0]+":").length();
			String value = result[4].substring(len);
			arr2[1] = value;
			if(arr2[0].equals("date")) {
				//cell.setValue(timeStampToDate(arr2[1]));
				cell.setValue("'" + arr2[1] + "'");
			} else if(arr2[0].equals("time")) {
				//cell.setValue(timeStampToDate(arr2[1]));
				cell.setValue("'" + arr2[1] + "'");
			} else if(arr2[0].equals("datetime")) {
				//cell.setValue(timeStampToDate(arr2[1]));
				cell.setValue("'" + arr2[1] + "'");
			} else if(arr2[0].equals("precisedatetime")) {
				//cell.setValue(timeStampToDate(arr2[1]));
				cell.setValue("'" + arr2[1] + "'");
			} else if(arr2[0].equals("varchar")) {
				
				//给单引号加上转义字符
				if(arr2[1].length()>2 && !isClob(oldColumn)) {
					String src = arr2[1].substring(1,arr2[1].length() - 1);
					src = src.replace("'", "''");
					arr2[1] = "'" + src + "'";
				}
				
				cell.setValue(arr2[1]);
			} 
			else {
				cell.setValue(arr2[1]);
			}
		}
		
		//主键名
		String[] arr3 = result[5].split(String.valueOf(oo));
		for (int i = 0; i < arr3.length; i++) {
			String[] temp = arr3[i].split(":");
			if(temp.length == 1)
				cell.getRowkeyName().add("null");
			else {
				if(TableMap.columnRename(oldTable,temp[1])) {
					//需要移除这一列
					if(TableMap.isDeleteColumn(oldTable,temp[1])) {
						cell.getRowkey().remove(i);//把对应的值也删除，不增加主键名
						LoggerTool.warn("remove rowkey,it's dangrous, table:" + oldTable + " column:" + oldColumn , new Throwable().getStackTrace());
					} else {
						//cell.setColumn(TableMap.getNewColumnName(oldTable,temp[1]));
						cell.getRowkeyName().add(TableMap.getNewColumnName(oldTable,temp[1]));
					}
				} else {
					cell.getRowkeyName().add(temp[1]);
				}
			}
		}
		return cell;
	}
	
	
	public static boolean isClob(String column) {
		
		Pattern reg = Pattern.compile("(.*)?_clob([0-9]*)");
		
		Matcher mc = reg.matcher(column);
		if(mc.matches()) {
			return true;
		}
		return false;
		
	}
	
	@Deprecated
	private static String timeStampToDate(String str) {
		
		try {
			long  stamp = Long.parseLong(str);
			
			long k = Math.abs(stamp % 1000);
			Date date = new Date((stamp / 1000L));
			SimpleDateFormat sdf = null;
			
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");// 
			String dateStr = String.format("'%s%03d'", sdf.format(date),k);
			return dateStr;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	private static List<String> mergeCellToSql(List<ObCellInfo> finalOp) {
		List<String> result = new ArrayList<String>();
		if(SQLBUILDER != null) {
			//生成insert/delete等语句
			result.addAll(SQLBUILDER.buildDMLStatement(finalOp));
			//生成create table等语句
			result.addAll(SQLBUILDER.buildDDLStatement(finalOp));
		}
		return result;
	}
	
	
	
	
	
}
