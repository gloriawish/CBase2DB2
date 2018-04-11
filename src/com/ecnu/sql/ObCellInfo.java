package com.ecnu.sql;

import java.util.ArrayList;
import java.util.List;

public class ObCellInfo
{
	private long seq;
	private String opType;
    private String table;
    private String column;
    
    private List<ObObjType> rowkeyType = new ArrayList<ObObjType>();
    private List<String> rowkey = new ArrayList<String>();
    
    private List<String> rowkeyName = new ArrayList<String>();
    
    private ObObjType valueType;
    private String value;
    
	public long getSeq() {
		return seq;
	}
	public void setSeq(long seq) {
		this.seq = seq;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getOpType() {
		return opType;
	}
	public void setOpType(String opType) {
		this.opType = opType;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	public List<ObObjType> getRowkeyType() {
		return rowkeyType;
	}
	public void setRowkeyType(List<ObObjType> rowkeyType) {
		this.rowkeyType = rowkeyType;
	}
	public List<String> getRowkey() {
		return rowkey;
	}
	public void setRowkey(List<String> rowkey) {
		this.rowkey = rowkey;
	}
	
	public List<String> getRowkeyName() {
		return rowkeyName;
	}
	public void setRowkeyName(List<String> rowkeyName) {
		this.rowkeyName = rowkeyName;
	}
	public ObObjType getValueType() {
		return valueType;
	}
	public void setValueType(ObObjType valueType) {
		this.valueType = valueType;
	}
	
	public String getMapKey() {
		return table + ":" + rowkey + ":" + column;
	}
	public String getTableRowKey() {
		return table + ":" + rowkey;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(seq + " type:" + opType + " table:" + table + " column:" + column + " ");
		sb.append("rowkey Name:" + rowkeyName.toString() + " rowkey Value:" + rowkey.toString() + " ");
		sb.append("value type:" + valueType + " value:" + value);
		return sb.toString();
	}
    
}
