package io.github.oguzhantortop.elasticjdbc;

import java.util.Properties;



public class ConfigStore {
	
	private static ConfigStore configStore = null;
	
	private boolean authenticated = false;
	private String host;
	private String port;
	private Properties props;
	
	private ConfigStore()  {};
	
	public boolean isAuthenticated() {
		return authenticated;
	}



	public void setAuthenticated(boolean authenticated) {
		this.authenticated = authenticated;
	}



	public String getHost() {
		return host;
	}



	public void setHost(String host) {
		this.host = host;
	}



	public String getPort() {
		return port;
	}



	public void setPort(String port) {
		this.port = port;
	}



	public Properties getProps() {
		return props;
	}



	public void setProps(Properties props) {
		this.props = props;
	}



	public static ConfigStore getInstance() {
		if(null == configStore) {
			configStore = new ConfigStore();
		}
		return configStore;
	}
	
	
}
