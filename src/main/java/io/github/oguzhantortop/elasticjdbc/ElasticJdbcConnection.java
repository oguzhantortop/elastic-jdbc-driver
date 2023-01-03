package io.github.oguzhantortop.elasticjdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ElasticJdbcConnection implements Connection{
	

	public ElasticJdbcConnection() throws SQLException {
		open();
	}

	private void open() throws SQLException {

	}

	
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(this);
	}

	
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	
	public Statement createStatement() throws SQLException {
		return new ElasticJdbcStatement(this);
	}

	
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return new ElasticJdbcPreparedStatement(this,sql);
	}

	
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public String nativeSQL(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public void setAutoCommit(boolean autoCommit) throws SQLException {
	}

	
	public boolean getAutoCommit() throws SQLException {
		return true;
	}

	
	public void commit() throws SQLException {

	}

	
	public void rollback() throws SQLException {

	}

	
	public void close() throws SQLException {

	}

	
	public boolean isClosed() throws SQLException {
		return false;
	}

	
	public DatabaseMetaData getMetaData() throws SQLException {
		DatabaseMetaData dbmd = new ElasticJdbcDatabaseMetaData(this);
		return dbmd;
	}

	
	public void setReadOnly(boolean readOnly) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	
	public void setCatalog(String catalog) throws SQLException {
	}

	
	public String getCatalog() throws SQLException {
		return null;
	}

	
	public void setTransactionIsolation(int level) throws SQLException {
	}

	
	public int getTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	
	public void clearWarnings() throws SQLException {
	}

	
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		if(resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY - CONCUR_READ_ONLY is supported");
		}
		return new ElasticJdbcStatement(this);
	}

	
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return null;
	}

	
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public void setHoldability(int holdability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		if(resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY - CONCUR_READ_ONLY is supported");
		}
		return new ElasticJdbcPreparedStatement(this, sql);
	}

	
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		if(autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
			throw new SQLFeatureNotSupportedException("Only NO_GENERATED_KEYS is supported");
		}
		return new ElasticJdbcPreparedStatement(this, sql);
	}

	
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public boolean isValid(int timeout) throws SQLException {
		//return isOpen();
		return true;
	}

	
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		
	}

	
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		
	}

	
	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Properties getClientInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public String getSchema() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	public void abort(Executor executor) throws SQLException {
		close();
	}

	
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		// not implemented
	}

	
	public int getNetworkTimeout() throws SQLException {
		return 0;
	}

	public boolean isOpen() {
		return true;
	}
}
