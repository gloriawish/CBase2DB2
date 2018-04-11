package com.ecnu.tool;

import java.util.List;

import net.sf.json.JSONObject;

public class TranstationSQL
{
    private String type=null;
    private String select=null;
    private String insert=null;
    private String update=null;
    private String delete=null;
    private String ddl=null;
    private String table = null;
    private String db = null;
    private String pk = null;
    private List<String> clob=null;
    public List<String> getClob() {
		return clob;
	}
	public void setClob(List<String> clob) {
		this.clob = clob;
	}
	public String toString()
    {
        return JSONObject.fromObject(this).toString();
    }
    public String getType()
    {
        return type;
    }
    public void setType(String type)
    {
        this.type = type;
    }
    public String getSelect()
    {
        return select;
    }
    public void setSelect(String select)
    {
        this.select = select;
    }
    public String getInsert()
    {
        return insert;
    }
    public void setInsert(String insert)
    {
        this.insert = insert;
    }
    public String getUpdate()
    {
        return update;
    }
    public void setUpdate(String update)
    {
        this.update = update;
    }
    public String getDelete()
    {
        return delete;
    }
    public void setDelete(String delete)
    {
        this.delete = delete;
    }
	public void setDdl(String ddl) {
		this.ddl = ddl;
	}
	public String getDdl() {
		return ddl;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getDb() {
		return db;
	}
	public void setDb(String db) {
		this.db = db;
	}
	public String getPk() {
		return pk;
	}
	public void setPk(String pk) {
		this.pk = pk;
	}
    
}

