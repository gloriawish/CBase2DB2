package com.ecnu.test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ecnu.sql.CBaseSQLBuilder;
import com.ecnu.sql.SqlGenerate;
import com.ecnu.tool.TranstationSQL;

public class TestSQLBuilder {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test_getClobName() {
		
		CBaseSQLBuilder b = new CBaseSQLBuilder();
		
		Assert.assertEquals("tran_content",b.getClobName("tran_content_clob11"));
		
	}
	
	@Test
	public void test_isClob() {
		
		Assert.assertTrue(SqlGenerate.isClob("tran_content_clob11"));
		
		TranstationSQL T =  new TranstationSQL();
		List<String> list = new ArrayList<>();
		list.add("{\"a\":123}");
		T.setClob(list);
		
		System.out.println(T.toString());
	}
	
	@Test
	public void test_sub() {
		String line = "transactionend:123 [123,1]";
		String[] array = line.substring(line.indexOf("[") + 1,line.length() - 1).split(",");
		for (int i = 0; i < array.length; i++) {
			System.out.println(array[i]);
		}
	}
	
	@Test
	public void test_time() {
		String str = "-61704230752000000";
		try {
			long  stamp = Long.parseLong(str);
			
			stamp -= 86048000000L;
			
			long k = stamp % 1000;
			
			Date date = new Date((stamp / 1000L));
			SimpleDateFormat sdf = null;
			
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");// 
			String dateStr = String.format("'%s%03d'", sdf.format(date),k);
			
			System.out.println(dateStr);
			
			long t = sdf.parse("0014-09-04 00:00:00.000").getTime();
			
			System.out.println(t);
			
			System.out.println(stamp);
			System.out.println((stamp / 1000L));
			System.out.println((stamp / 1000L) - t);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
	}


}
