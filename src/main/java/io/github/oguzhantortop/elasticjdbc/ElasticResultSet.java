package io.github.oguzhantortop.elasticjdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

public class ElasticResultSet implements ResultSet {
	static final String INVALID_COLUMN_NAME = "invalid column name";
	static final String INVALID_COLUMN_INDEX = "invalid column index";
	static final DateFormat DATE_ISO_8601 = new SimpleDateFormat("yyyy-MM-dd");
	private List<LinkedHashMap<String, Object>> result = null;
	private int cursor = -1;
	private RowSetMetaData metaData;
	private boolean wasNull = false;
	private int fetchDirection = ResultSet.FETCH_FORWARD;
	private int iterationStep = 1;
	private int fetchSize = 1000;
	private String scroll = "";

	public ElasticResultSet() {
	}

	/**
	 * @param in source from where the CSV data will be read
	 * 
	 * @throws SQLException any exception will be wrapped on SQLException, so that
	 *                      it is not necessary to add additional catches to client
	 *                      code.
	 */
	public ElasticResultSet(String sqlResponse) throws SQLException {
		loadElasticData(sqlResponse);
	}
	

	public ElasticResultSet(List<? extends ResultSetColumnDescriptor> columnDescriptors, List<List<Object>> dataRows) throws SQLException {
		try {
			createMetaData(columnDescriptors);
			createResultSet(columnDescriptors, dataRows);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	private void createResultSet(List<? extends ResultSetColumnDescriptor> columnDescriptors,
			List<List<Object>> dataRows) {
		List<LinkedHashMap<String, Object>> res = new ArrayList<>();
		for (List<Object> row : dataRows) {
			LinkedHashMap<String, Object> map = new LinkedHashMap<>();
			int index = 0;
			if(!row.isEmpty()) {
				for (ResultSetColumnDescriptor col : columnDescriptors) {
					map.put(col.getName(), row.get(index));
					index++;
				}
			}
			
			res.add(map);
		}
		this.result = res;
	}

	private void createMetaData(List<? extends ResultSetColumnDescriptor> columnDescriptors) throws SQLException {
		RowSetMetaData rsMD = new RowSetMetaDataImpl();
		int index = 1;
		rsMD.setColumnCount(columnDescriptors.size());
		for (ResultSetColumnDescriptor col : columnDescriptors) {
			/*
			 * boolean composite date double geo_point ip keyword long
			 */
			String colName = col.getName();
			String type = col.getType();
			//String label = col.getLabel();
			rsMD.setColumnName(index, colName);
			rsMD.setColumnLabel(index, colName);
			ElasticFieldType sqlType = getSqlType(type);
				rsMD.setColumnType(index, sqlType.getSqlTypeCode());
				rsMD.setPrecision(index, sqlType.getPrecision());
				rsMD.setCaseSensitive(index, sqlType.isCaseSensitive());
				rsMD.setSigned(index, !sqlType.isUnsigned());
				if(sqlType.getSqlType()!=null)
					rsMD.setColumnTypeName(index,sqlType.getSqlType());
				else
					rsMD.setColumnTypeName(index,"null");
			index++;
		}
		this.metaData = rsMD;
		
		
	}

	private void loadElasticData(String sqlResponse) throws SQLException {
		try {
			System.out.println(sqlResponse);
			iterationStep = 0;
			scroll  = "";
			Gson gson = new Gson();
			// This converts your String into whatever Type you want. In this case, we are
			// using JsonObjects from GSON.
			JsonObject json = gson.fromJson(sqlResponse, JsonObject.class);
			
			if(json.has("cursor")) {
				scroll = json.get("cursor").getAsString();
			}

			// Now, we want to get the Json Array from the JsonObject we just created above
			if(this.metaData == null) {
				
				JsonArray columns = json.get("columns").getAsJsonArray();
				Iterator<JsonElement> colIter = columns.iterator();
				RowSetMetaData rsMD = new RowSetMetaDataImpl();
				int index = 1;
				rsMD.setColumnCount(columns.size());
				while (colIter.hasNext()) {
					JsonElement colEl = colIter.next();
					String colName = ((JsonObject) colEl).get("name").getAsString().toString();
					String type = ((JsonObject) colEl).get("type").getAsString().toString();
					rsMD.setColumnName(index, colName);
					if(type.equals("datetime"))
						type = "date";
					ElasticFieldType sqlType = getSqlType(type);
					rsMD.setColumnType(index, sqlType.getSqlTypeCode());
					rsMD.setPrecision(index, sqlType.getPrecision());
					rsMD.setCaseSensitive(index, sqlType.isCaseSensitive());
					rsMD.setSigned(index, !sqlType.isUnsigned());
					rsMD.setColumnTypeName(index,sqlType.getSqlType());
					index++;
				}
				this.metaData = rsMD;
			}

			JsonArray rows = json.get("rows").getAsJsonArray();
			Iterator<JsonElement> iter = rows.iterator();
			JsonArray resultArr = new JsonArray();
			while (iter.hasNext()) {
				JsonElement el = iter.next();
				JsonObject temp = new JsonObject();
				
				for (int i = 1; i <= this.metaData.getColumnCount(); i++) {
					temp.add(this.metaData.getColumnName(i), ((JsonArray) el).get(i-1));
				}
				
				resultArr.add(temp);
			}
			Type resultType = new TypeToken<List<LinkedHashMap<String, Object>>>() {
			}.getType();
			result = gson.fromJson(resultArr.toString(), resultType);

		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	private ElasticFieldType getSqlType(String type) {
		if(type !=null && !type.equals(""))
			return ElasticFieldType.resolveByElasticType(type);
		else 
			return ElasticFieldType.resolveByElasticType("UNKNOWN");
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void afterLast() throws SQLException {
		cursor = result.size();
	}

	@Override
	public void beforeFirst() throws SQLException {
		cursor = -1;
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}


	@Override
	public void close() throws SQLException {
		// noop
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}


	@Override
	public boolean first() throws SQLException {
		if (result.size() > 0) {
			cursor = 0;
			return true;
		}

		return false;
	}

	

	@Override
	public void insertRow() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean isClosed() throws SQLException {
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean isLast() throws SQLException {
		final boolean hasNext = (cursor + 1) < result.size();
		return !hasNext;
	}

	@Override
	public boolean last() throws SQLException {
		if (result.size() > 0) {
			cursor = result.size() - 1;
			return true;
		}

		return false;
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean next() throws SQLException {

		boolean hasNext = (cursor + 1) < result.size();
		if (hasNext) {
			cursor++;
		} else if (!scroll.isEmpty()) {
			loadElasticData(ElasticUtil.scrollElastic(scroll));
			if(!result.isEmpty()) {
				cursor  = 0;
				hasNext =true;
			}
		}

		return hasNext;
	}

	@Override
	public boolean previous() throws SQLException {
		cursor--;

		if (cursor < -1) {
			cursor = -1; // one row before the first is the limit
		}

		final boolean beforeFirst = (cursor < 0);
		return !beforeFirst;
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}


	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNClob(int columnIndex, NClob clob) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNClob(String columnLabel, NClob clob) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNString(int columnIndex, String string) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNString(String columnLabel, String string) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException("to be implemented");
	}
	
	private <T> T getColumnValue(int columnIndex, Class<T> clazz, T nullValue) throws SQLException {
		if ((cursor < 0) || (cursor >= result.size())) {
			throw new SQLException("cursor not pointing to a valid row");
		}

		LinkedHashMap<String, Object> row = result.get(cursor);
		if ((columnIndex < 0) || (columnIndex > row.size())) {
			throw new SQLException(INVALID_COLUMN_INDEX);
		}
		try {
			T value =(T) row.values().toArray()[columnIndex - 1];
			wasNull = value == null;
			return wasNull ? nullValue :  value;
		} catch(NullPointerException e) {
			wasNull = true;
			return nullValue;
		}
	}
	
	private <T> T getColumnValue(String columnName, Class<T> clazz, T nullValue) throws SQLException {
		if ((cursor < 0) || (cursor >= result.size())) {
			throw new SQLException("cursor not pointing to a valid row");
		}

		LinkedHashMap<String, Object> row = result.get(cursor);
		if (!row.containsKey(columnName)) {
			throw new SQLException(INVALID_COLUMN_NAME);
		}
		try {
			T value = (T) row.get(columnName);
			wasNull = value == null;
			return wasNull ? nullValue : (T) value;
		} catch(NullPointerException e) {
			wasNull = true;
			return nullValue;
		}
	}

	@Override
	public boolean wasNull() throws SQLException {
		return wasNull;
	}
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, String.class, null);
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Boolean.class, false);
	}
	
	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Byte.class, (byte)0);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Short.class, (short)0);
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Integer.class, 0);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Long.class, 0L);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Float.class, (float)0.0);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Double.class, 0.0);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return getColumnValue(columnIndex, BigDecimal.class, new BigDecimal(0));
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Date.class, null);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Time.class, null);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Timestamp.class, null);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, String.class, null);
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Boolean.class, false);
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Byte.class, (byte)0);
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Short.class, (short)0);
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Integer.class, 0);
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Long.class, 0L);
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Float.class, (float)0.0);
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Double.class, 0.0);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getColumnValue(columnLabel, BigDecimal.class, new BigDecimal(0));
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Date.class, null);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Time.class, null);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Timestamp.class, null);
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public String getCursorName() throws SQLException {
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return this.metaData;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, Object.class, null);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Object.class, null);
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, Integer.class, 0);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		String data = getColumnValue(columnIndex, String.class, null);
		return data == null ? null : new StringReader(data);
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		String data = getColumnValue(columnLabel, String.class, null);
		return data == null ? null : new StringReader(data);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex, BigDecimal.class, new BigDecimal(0));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getColumnValue(columnLabel, BigDecimal.class, new BigDecimal(0));
	}

	@Override
	public int getRow() throws SQLException {
		return this.cursor;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		this.fetchDirection = direction;

		switch (direction) {
			case ResultSet.FETCH_REVERSE:
				this.iterationStep = -1;
				break;
			case ResultSet.FETCH_FORWARD:
			default:
				this.iterationStep  = 1;
				break;
		}
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return fetchDirection;
	}

	@Override
	public int getFetchSize() throws SQLException {
		return this.fetchSize;
	}


	@Override
	public int getType() throws SQLException {
		return  ResultSet.TYPE_SCROLL_SENSITIVE;
	}

	@Override
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public Statement getStatement() throws SQLException {
		return null;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException { // !!
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException { // !!
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException { // !!
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException { // !!
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { // !!
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { // !!
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}


}
