package io.github.oguzhantortop.elasticjdbc.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

public class ElasticDriverTest {
	
	
	@Test
	public void testSelectQuery() {
		try {
			Class.forName("io.github.oguzhantortop.elasticjdbc.ElasticJdbcDriver");
			Connection conn = DriverManager
			        .getConnection("jdbc:elasticjdbc:http://20.67.35.226:7001",
			                "elastic", "7wqP26.QyMZpexDIx");
			DatabaseMetaData metaData = conn.getMetaData();
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery("SELECT * from \"kurumtürk_million\" limit 1000");
			
			//ResultSet rs = metaData.getColumns(null, null, "dumb_with_join",null);//.getTables(null,"docker-cluster",null,null);
			
//			Class.forName("org.fpasti.jdbc.esqlj.EsDriver");
//			Connection conn2 = DriverManager
//			        .getConnection("jdbc:esqlj:http://localhost:9201");
//			DatabaseMetaData metaData2 = conn2.getMetaData();
//			ResultSet rs2 = metaData2.getSchemas(null, "%"); //.getTables(null,"docker-cluster",null,null);
//			
//			System.out.println("***************************************************");
//			
//			//ResultSet rs = statement.executeQuery("select * from \"kurumtürk_million\"");
//			//ResultSet rs = statement.executeQuery("SELECT DAY( CURDATE())");
			//rs = statement.executeQuery("SELECT DAY( CURDATE())");
//			for(int i =1 ;i<= rs.getMetaData().getColumnCount();i++) {
//				System.out.println("kolon etiket: "+rs.getMetaData().getColumnLabel(i)+" kolon ismi: "+rs.getMetaData().getColumnName(i)+" kolon tipi: "+rs.getMetaData().getColumnTypeName(i));
//			}
//			
//			System.out.println("***************************************************");
//			
//			for(int i =1 ;i<= rs2.getMetaData().getColumnCount();i++) {
//				System.out.println("kolon etiket: "+rs2.getMetaData().getColumnLabel(i)+" kolon ismi: "+rs2.getMetaData().getColumnName(i)+" kolon tipi: "+rs2.getMetaData().getColumnTypeName(i));
//			}
			
			int i = 0;
//			
			while(rs.next()) {
//				for(int i =1 ;i< rs.getMetaData().getColumnCount();i++) {
//					
//					System.out.print(i);
//				}
				System.out.println(i++);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}
	}
	
	
}
