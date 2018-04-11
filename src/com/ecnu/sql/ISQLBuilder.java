package com.ecnu.sql;

import java.util.List;

public interface ISQLBuilder {
	
	
	public List<String> buildDMLStatement(List<ObCellInfo> finalOp);
	
	public List<String> buildDDLStatement(List<ObCellInfo> finalOp);

}
