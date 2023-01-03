package io.github.oguzhantortop.elasticjdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class ElasticJdbcDriver implements Driver {
	
	private Properties info;
	
	

	public Properties getInfo() {
		return info;
	}

	public void setInfo(Properties info) {
		this.info = info;
	}

	static {
		try {
			DriverManager.registerDriver(new ElasticJdbcDriver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Connection connect(String url, Properties info) throws SQLException {
		this.info = info;
		String netUri = url.replace("jdbc:elasticjdbc:", "");
		String[] arr = netUri.split(":");
		ConfigStore.getInstance().setHost(arr[0]+":"+arr[1]);
		ConfigStore.getInstance().setPort(arr[2]);
		if(info.containsKey("user")) {
			ConfigStore.getInstance().setAuthenticated(true);
		}
		ConfigStore.getInstance().setProps(info);
		
		
		return new ElasticJdbcConnection();
	}

	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith("jdbc:elasticjdbc:");
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return null;
	}

	public int getMajorVersion() {
		return 0;
	}

	public int getMinorVersion() {
		return 0;
	}

	public boolean jdbcCompliant() {
		return false;
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}
	

}
