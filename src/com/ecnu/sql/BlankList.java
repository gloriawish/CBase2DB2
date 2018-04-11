package com.ecnu.sql;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlankList {
	
	
	public static Set<String> BLANK = new HashSet<>();
	
	static {
		BLANK.add("__first_tablet_entry");
		BLANK.add("__all_all_column");
		BLANK.add("__all_join_info");
		BLANK.add("__all_server_stat");
		BLANK.add("__all_sys_param");
		BLANK.add("__all_sys_config");
		BLANK.add("__all_sys_stat");
		BLANK.add("__all_user");
		BLANK.add("__all_table_privilege");
		BLANK.add("__all_trigger_event");
		BLANK.add("__all_sys_config_stat");
		BLANK.add("__all_server_session");
		BLANK.add("__all_cluster");
		BLANK.add("__all_server");
		BLANK.add("__all_client");
		BLANK.add("__tables_show");
		BLANK.add("__index_show");
		BLANK.add("__all_truncate_op");
		BLANK.add("__databases_show");
		BLANK.add("__variables_show");
		BLANK.add("__create_table_show");
		BLANK.add("__table_status_show");
		BLANK.add("__schema_show");
		BLANK.add("__columns_show");
		BLANK.add("__server_status_show");
		BLANK.add("__parameters_show");
		BLANK.add("__all_statement");
		BLANK.add("__all_ddl_operation");
		BLANK.add("__all_database");
		BLANK.add("__all_database_privilege");
		BLANK.add("__index_process_info");
		BLANK.add("ob_virtual_col");
		BLANK.add("__all_cchecksum_info");
		BLANK.add("__all_sequence");
		
	}
	
	public static boolean inBlank(String tableName) {
		
		Pattern regx = Pattern.compile("__([0-9]*)__idx__(.*)");
		Matcher m = regx.matcher(tableName);
		if(m.find()) {
			return true;
		}
		if(BLANK.contains(tableName)) {
			return true;
		}
		return false;
	}
	
	public static int inCreateTable(String tableName) {
		
		if(tableName.equals("__all_sys_stat"))
			return 1;
		if(tableName.equals("__all_ddl_operation"))
			return 2;
		if(tableName.equals("__first_tablet_entry"))
			return 3;
		if(tableName.equals("__all_all_column"))
			return 4;
		if(tableName.equals("__all_table_privilege"))
			return 5;
		if(tableName.equals("__all_trigger_event"))
			return 6;
		return -1;
		
	}
	
	public static int inDeleteTable(String tableName) {
		
		if(tableName.equals("__first_tablet_entry"))
			return 1;
		if(tableName.equals("__all_all_column"))
			return 2;
		if(tableName.equals("__all_ddl_operation"))
			return 3;
		if(tableName.equals("__all_table_privilege"))
			return 4;
		return -1;
		
	}
	
	public static boolean isDatabase(String tableName) {
		
		if(tableName.equals("__all_database"))
			return true;
		
		return false;
		
	}
	
}
