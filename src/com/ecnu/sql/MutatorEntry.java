package com.ecnu.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

//一个ObMutator的所有操作
public class MutatorEntry {
	
	private long seq; 
	
	//一个mutator中的所有操作
	private List<ObCellInfo> cells = new ArrayList<ObCellInfo>();

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public void addCell(ObCellInfo cell) {
		cell.setSeq(seq);//设置seq
		this.cells.add(cell);
	}
	
	public List<ObCellInfo> getCells() {
		return cells;
	}

	public void setCells(List<ObCellInfo> cells) {
		this.cells = cells;
	}
	
	
	/**
	 * 最终操作序列
	 * @return
	 */
	public Map<String, List<ObCellInfo>> finalOption() {
		
		//LinkedHashMap保证有顺序
		Map<String, List<ObCellInfo>> finalOp = new LinkedHashMap<String, List<ObCellInfo>>();
		
		List<ObCellInfo> finalList = new ArrayList<ObCellInfo>();
		
		Map<String,ObCellInfo> finalTable = new LinkedHashMap<String,ObCellInfo>();
		
		for (int i = 0; i < cells.size(); i++) {
			ObCellInfo item = cells.get(i);
			
			Iterator<Entry<String, ObCellInfo>> it = finalTable.entrySet().iterator();
			if(item.getOpType().equalsIgnoreCase("DEL_ROW")) {
				while(it.hasNext()) {
					Entry<String, ObCellInfo> pair = it.next();
					//表名和主键相等，则从表里面删除
					if(pair.getValue().getTable().equals(item.getTable()) &&
							pair.getValue().getRowkey().equals(item.getRowkey())) {
						it.remove();
					}
				}
				//最终列表里添加一条删除的Cell
				finalList.add(item);
			} else {
				
				String key = item.getMapKey();
				
				if(finalTable.containsKey(key)) {//存在条目则直接改最终值
					finalTable.get(key).setValue(item.getValue());
				} else {
					finalTable.put(key, item);
				}
			}
		}
		//获取最终操作列表
		for (Entry<String, ObCellInfo> pair : finalTable.entrySet()) {
			finalList.add(pair.getValue());
		}
		
		//分组，分为对每个rowkey操作的序列
		for (ObCellInfo obCellInfo : finalList) {
			if(finalOp.containsKey(obCellInfo.getTableRowKey())) {
				finalOp.get(obCellInfo.getTableRowKey()).add(obCellInfo);
			} else {
				finalOp.put(obCellInfo.getTableRowKey(), new ArrayList<ObCellInfo>());
				finalOp.get(obCellInfo.getTableRowKey()).add(obCellInfo);
			}
		}
		
		return finalOp;
	}
	
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("begin mutator:" + this.seq);
		sb.append("\n");
		for (int i = 0; i < this.cells.size(); i++) {
			sb.append(cells.get(i).toString());
			sb.append("\n");
		}
		sb.append("end mutator:" + this.seq);
		return sb.toString();
	}

}
