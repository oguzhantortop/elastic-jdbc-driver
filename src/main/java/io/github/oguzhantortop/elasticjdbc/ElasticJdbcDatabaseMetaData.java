package io.github.oguzhantortop.elasticjdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ElasticJdbcDatabaseMetaData implements DatabaseMetaData {

	private ElasticJdbcConnection connection;

	public ElasticJdbcDatabaseMetaData(ElasticJdbcConnection connection) {
		this.connection = connection;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException {
		return "";
		// return connection.getUrl();
	}

	@Override
	public String getUserName() throws SQLException {
		return "";
		// return connection.getUser();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "ElasticSearchJDBC";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return "";
		// return connection.getClusterMetadata().getVersion().getFullVersion();
	}

	@Override
	public String getDriverName() throws SQLException {
		return "OpenSearch JDBC Driver";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return "";
		// return Version.Current.getFullVersion();
	}

	@Override
	public int getDriverMajorVersion() {
		return 1;
		// return Version.Current.getMajor();
	}

	@Override
	public int getDriverMinorVersion() {
		return 11;
		// return Version.Current.getMinor();
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		// space to indicate quoting not supported currently
		return "\"";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return "";
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "\\";
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "";
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			throws SQLException {
		return null;
		/*return emptyResultSet(rscd("PROCEDURE_CAT"), rscd("PROCEDURE_SCHEM"), rscd("PROCEDURE_NAME"), rscd("RESERVED4"),
				rscd("RESERVED5"), rscd("RESERVED6"), rscd("REMARKS"), rscd("PROCEDURE_TYPE", "short"),
				rscd("SPECIFIC_NAME"));*/
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {
		return null;
//		return emptyResultSet(rscd("PROCEDURE_CAT"), rscd("PROCEDURE_SCHEM"), rscd("PROCEDURE_NAME"),
//				rscd("COLUMN_NAME"), rscd("COLUMN_TYPE", "Short"), rscd("DATA_TYPE", "Integer"), rscd("TYPE_NAME"),
//				rscd("PRECISION", "Integer"), rscd("LENGTH", "Integer"), rscd("SCALE", "Short"), rscd("RADIX", "Short"),
//				rscd("NULLABLE", "Short"), rscd("REMARKS"), rscd("COLUMN_DEF"), rscd("SQL_DATA_TYPE", "Integer"),
//				rscd("SQL_DATETIME_SUB", "Integer"), rscd("CHAR_OCTET_LENGTH", "Integer"),
//				rscd("ORDINAL_POSITION", "Integer"), rscd("IS_NULLABLE"), rscd("SPECIFIC_NAME"));
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		ResultSet resultSet;


		List<ResultSetColumnDescriptor> columnDescriptors = new ArrayList<>();
		columnDescriptors.add(rscd("TABLE_CAT",""));
		columnDescriptors.add(rscd("TABLE_SCHEM", ""));
		columnDescriptors.add(rscd("TABLE_NAME", "text"));
		columnDescriptors.add(rscd("TABLE_TYPE","text"));
		columnDescriptors.add(rscd("REMARKS","text"));
		columnDescriptors.add(rscd("TYPE_CAT",""));
		columnDescriptors.add(rscd("TYPE_SCHEM", ""));
		columnDescriptors.add(rscd("TYPE_NAME", ""));
		columnDescriptors.add(rscd("SELF_REFERENCING_COL_NAME", ""));
		columnDescriptors.add(rscd("REF_GENERATION", ""));

		List<List<Object>> dataRows = new ArrayList<>();
		
		String tablesResponse = ElasticUtil.queryElasticSearch("show tables");
		
		Gson gson = new Gson();
		// This converts your String into whatever Type you want. In this case, we are
		// using JsonObjects from GSON.
		JsonObject json = gson.fromJson(tablesResponse, JsonObject.class);
		// Now, we want to get the Json Array from the JsonObject we just created above
		JsonArray rows = json.get("rows").getAsJsonArray();
		
		Iterator<JsonElement> iter = rows.iterator();
		while (iter.hasNext()) {
			JsonElement el = iter.next();
			dataRows.add(Arrays.asList(null, null, ((JsonArray) el).get(0).getAsString(), ((JsonArray) el).get(1).getAsString(), "", null, null, null, null, null));
		}
		
		

		resultSet = new ElasticResultSet(columnDescriptors, dataRows);

		return resultSet;
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		List<ResultSetColumnDescriptor> columnDescriptors = new ArrayList<>();
		columnDescriptors.add(rscd("TABLE_SCHEM","text"));
		List<List<Object>> dataRows = new ArrayList<>();
		dataRows.add(Arrays.asList("docker-cluster"));
		return new ElasticResultSet(columnDescriptors, dataRows);
	}
	

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {

		ResultSet resultSet = getSchemasX(catalog, schemaPattern);

		return resultSet;
	}

	private ResultSet getSchemasX(String catalog, String schemaPattern) throws SQLException {
		List<ResultSetColumnDescriptor> columnDescriptors = new ArrayList<>();
		columnDescriptors.add(rscd("TABLE_SCHEM"));
		columnDescriptors.add(rscd("TABLE_CATALOG"));

		List<List<Object>> dataRows = new ArrayList<>();
		dataRows.add(new ArrayList<>());
		return new ElasticResultSet(columnDescriptors, dataRows);
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		ResultSet resultSet = null;

		List<ResultSetColumnDescriptor> columnDescriptors = new ArrayList<>();
		columnDescriptors.add(rscd("TABLE_CAT"));

		List<List<Object>> dataRows = new ArrayList<>();
		dataRows.add(Arrays.asList(""));

		resultSet = new ElasticResultSet(columnDescriptors, dataRows);

		return resultSet;
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {

		ResultSet resultSet = null;

		List<ResultSetColumnDescriptor> columnDescriptors = new ArrayList<>();
		columnDescriptors.add(rscd("TABLE_TYPE",""));

		List<List<Object>> dataRows = new ArrayList<>();
		dataRows.add(Arrays.asList("TABLE"));
		dataRows.add(Arrays.asList("VIEW"));

		resultSet = new ElasticResultSet(columnDescriptors, dataRows);

		return resultSet;
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		
		List<ResultSetColumnDescriptor> columnDescriptors = new ArrayList<>();
		columnDescriptors.add(rscd("TABLE_SCHEM",""));
		columnDescriptors.add(rscd("TABLE_NAME", "text"));
		columnDescriptors.add(rscd("COLUMN_NAME", "text"));
		columnDescriptors.add(rscd("DATA_TYPE", "integer"));
		columnDescriptors.add(rscd("TYPE_NAME", "text"));
		columnDescriptors.add(rscd("COLUMN_SIZE","integer"));
		columnDescriptors.add(rscd("BUFFER_LENGTH",""));
		columnDescriptors.add(rscd("DECIMAL_DIGITS",""));
		columnDescriptors.add(rscd("NUM_PREC_RADIX","integer"));
		columnDescriptors.add(rscd("NULLABLE","integer"));
		columnDescriptors.add(rscd("REMARKS",""));
		columnDescriptors.add(rscd("COLUMN_DEF",""));
		columnDescriptors.add(rscd("SQL_DATA_TYPE",""));
		columnDescriptors.add(rscd("SQL_DATETIME_SUB",""));
		columnDescriptors.add(rscd("CHAR_OCTET_LENGTH",""));
		columnDescriptors.add(rscd("ORDINAL_POSITION",""));
		columnDescriptors.add(rscd("IS_NULLABLE","text"));
		columnDescriptors.add(rscd("SCOPE_CATALOG",""));
		columnDescriptors.add(rscd("SCOPE_SCHEMA",""));
		columnDescriptors.add(rscd("SCOPE_TABLE",""));
		columnDescriptors.add(rscd("SOURCE_DATA_TYPE","integer"));
		columnDescriptors.add(rscd("IS_AUTOINCREMENT","text"));
		columnDescriptors.add(rscd("IS_GENERATEDCOLUMN",""));
		
		List<List<Object>> dataRows = new ArrayList<>();
		
		String columnsResponse = ElasticUtil.queryElasticSearch("SHOW COLUMNS IN \"$\"".replace("$", tableNamePattern));
		Gson gson = new Gson();
		// This converts your String into whatever Type you want. In this case, we are
		// using JsonObjects from GSON.
		JsonObject json = gson.fromJson(columnsResponse, JsonObject.class);
		// Now, we want to get the Json Array from the JsonObject we just created above
		JsonArray rows = json.get("rows").getAsJsonArray();
		
		Iterator<JsonElement> iter = rows.iterator();
		while (iter.hasNext()) {
			JsonElement el = iter.next();
			String typeStr = ((JsonArray) el).get(2).getAsString();
			if(typeStr.equals("unsupported") || ((JsonArray) el).get(0).getAsString().contains(".keyword"))
				continue;
			if(typeStr.equals("datetime"))
				typeStr = "date";
			ElasticFieldType type = ElasticFieldType.resolveByElasticType(typeStr);
			dataRows.add(Arrays.asList(null, tableNamePattern, ((JsonArray) el).get(0).getAsString(),type.getSqlTypeCode() , type.getSqlType(), (type.equals(ElasticFieldType.KEYWORD))?32766L:null, null, null, 10,ResultSetMetaData.columnNullable,
					null,null,null,null,null,null,"YES",null,null,null,type.getSqlTypeCode(),"NO",null));
		}
		
		

		ElasticResultSet resultSet = new ElasticResultSet(columnDescriptors, dataRows);
		
		return resultSet;
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException {
		return null;
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException {
		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException {
		return null;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		return null;
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		return null;
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return null;
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return null;
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		return null;
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		ResultSet resultSet = null;

		List<ResultSetColumnDescriptor> columnDescriptors = new ArrayList<>();
		columnDescriptors.add(rscd("TYPE_NAME","text"));
		columnDescriptors.add(rscd("DATA_TYPE", "integer"));
		columnDescriptors.add(rscd("PRECISION", "integer"));
		columnDescriptors.add(rscd("LITERAL_PREFIX","text"));
		columnDescriptors.add(rscd("LITERAL_SUFFIX","text"));
		columnDescriptors.add(rscd("CREATE_PARAMS",""));
		columnDescriptors.add(rscd("NULLABLE", "boolean"));
		columnDescriptors.add(rscd("CASE_SENSITIVE", "boolean"));
		columnDescriptors.add(rscd("SEARCHABLE", "short"));
		columnDescriptors.add(rscd("UNSIGNED_ATTRIBUTE", "boolean"));
		columnDescriptors.add(rscd("FIXED_PREC_SCALE", "boolean"));
		columnDescriptors.add(rscd("AUTO_INCREMENT", "boolean"));
		columnDescriptors.add(rscd("LOCAL_TYPE_NAME","text"));
		columnDescriptors.add(rscd("MINIMUM_SCALE", ""));
		columnDescriptors.add(rscd("MAXIMUM_SCALE", ""));
		columnDescriptors.add(rscd("SQL_DATA_TYPE", ""));
		columnDescriptors.add(rscd("SQL_DATETIME_SUB", ""));
		columnDescriptors.add(rscd("NUM_PREC_RADIX", "integer"));

		List<List<Object>> dataRows = new ArrayList<>();
		for (ElasticFieldType t : ElasticFieldType.values()) {
			dataRows.add(Arrays.asList(t.getSqlType(), t.getSqlTypeCode(), t.getPrecision(), t.getLiteralPrefix(),
					t.getLiteralSuffix(), null, true, t.isCaseSensitive(), 1, t.isUnsigned(), false, false,
					t.getSqlType(), null, null, null, null, 10));
		}
		dataRows.remove(dataRows.size()-1);

		resultSet = new ElasticResultSet(columnDescriptors, dataRows);

		return resultSet;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException {
		return null;
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		return concurrency == ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException {
		List<ResultSetColumnDescriptor> columnDescriptors = new ArrayList<>();
		columnDescriptors.add(rscd("TABLE_CAT",""));
		columnDescriptors.add(rscd("TYPE_SCHEM", ""));
		columnDescriptors.add(rscd("TYPE_NAME", "text"));
		columnDescriptors.add(rscd("CLASS_NAME", "text"));
		columnDescriptors.add(rscd("DATA_TYPE", "text"));
		columnDescriptors.add(rscd("REMARKS","text"));
		columnDescriptors.add(rscd("BASE_TYPE",""));
		
		
		List<List<Object>> dataRows = Arrays.stream(ElasticFieldType.values()).filter(t -> t.isUdt()).map(t -> new ArrayList<Object>(Arrays.asList(catalog, schemaPattern, t.getSqlType(), t.getClass().getName(), t.getSqlTypeCode(), null, null))).collect(Collectors.toList()); 
		
		
		return new ElasticResultSet(columnDescriptors, dataRows);

	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return true;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		return emptyResultSet(rscd("TYPE_CAT"), rscd("TYPE_SCHEM"), rscd("TYPE_NAME"), rscd("SUPERTYPE_CAT"),
				rscd("SUPERTYPE_SCHEM"), rscd("SUPERTYPE_NAME"));
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return emptyResultSet(rscd("TABLE_CAT"), rscd("TABLE_SCHEM"), rscd("TABLE_NAME"), rscd("SUPERTABLE_NAME"));
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException {
		return emptyResultSet(rscd("TYPE_CAT"), rscd("TYPE_SCHEM"), rscd("TYPE_NAME"), rscd("ATTR_NAME"),
				rscd("DATA_TYPE", "Integer"), rscd("ATTR_TYPE_NAME"), rscd("ATTR_SIZE", "Integer"),
				rscd("DECIMAL_DIGITS", "Integer"), rscd("NUM_PREC_RADIX", "Integer"), rscd("NULLABLE", "Integer"),
				rscd("REMARKS"), rscd("ATTR_DEF"), rscd("SQL_DATA_TYPE", "Integer"),
				rscd("SQL_DATETIME_SUB", "Integer"), rscd("CHAR_OCTET_LENGTH", "Integer"),
				rscd("ORDINAL_POSITION", "Integer"), rscd("IS_NULLABLE"), rscd("SCOPE_CATALOG"), rscd("SCOPE_SCHEMA"),
				rscd("SCOPE_TABLE"), rscd("SOURCE_DATA_TYPE", "Short"));
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return 1;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return 4;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return 2;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return 0;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return true;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_VALID_FOREVER;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return emptyResultSet(rscd("NAME"), rscd("MAX_LEN"), rscd("DEFAULT_VALUE"), rscd("DESCRIPTION") );
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
			throws SQLException {
		return emptyResultSet(rscd("FUNCTION_CAT"), rscd("FUNCTION_SCHEM"), rscd("FUNCTION_NAME"), rscd("REMARKS"),
				rscd("FUNCTION_TYPE", "Short"), rscd("SPECIFIC_NAME"));
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
			String columnNamePattern) throws SQLException {
		return emptyResultSet(rscd("FUNCTION_CAT"), rscd("FUNCTION_SCHEM"), rscd("FUNCTION_NAME"), rscd("COLUMN_NAME"),
				rscd("COLUMN_TYPE", "Short"), rscd("DATA_TYPE", "Integer"), rscd("TYPE_NAME"),
				rscd("PRECISION", "Integer"), rscd("LENGTH", "Integer"), rscd("SCALE", "Short"), rscd("RADIX", "Short"),
				rscd("NULLABLE", "Short"), rscd("REMARKS"), rscd("CHAR_OCTET_LENGTH", "Integer"),
				rscd("ORDINAL_POSITION", "Integer"), rscd("IS_NULLABLE"), rscd("SPECIFIC_NAME"));
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException {
		return emptyResultSet(rscd("TABLE_CAT"), rscd("TABLE_SCHEM"), rscd("TABLE_NAME"), rscd("COLUMN_NAME"),
				rscd("DATA_TYPE", "Integer"), rscd("COLUMN_SIZE", "Integer"), rscd("DECIMAL_DIGITS", "Integer"),
				rscd("NUM_PREC_RADIX", "Integer"), rscd("COLUMN_USAGE"), rscd("REMARKS"),
				rscd("CHAR_OCTET_LENGTH", "Integer"), rscd("IS_NULLABLE"));
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}

	static ResultSetColumnDescriptor rscd(String name) {
		return rscd(name, "text", null);
	}

	static ResultSetColumnDescriptor rscd(String name, String type) {
		return rscd(name, type, null);
	}

	static ResultSetColumnDescriptor rscd(String name, String type, String label) {
		return new ResultSetColumnDescriptor(name, type, label);
	}

	private String getClusterCatalogName() throws SQLException {
		return null;// connection.getClusterMetadata().getClusterName();
	}

	private boolean clusterCatalogMatches(String catalog) throws SQLException {
		return catalog == null || "%".equalsIgnoreCase(catalog) || catalog.equalsIgnoreCase(getClusterCatalogName());
	}

	private boolean clusterSchemaMatches(String schema) {
		return schema == null || schema.equals("*") || schema.equals("");
	}

	private static ElasticResultSet emptyResultSet(ResultSetColumnDescriptor... resultSetColumnDescriptors)
			throws SQLException {
		List<List<Object>> rows = new ArrayList<>(0);
		return new ElasticResultSet(Arrays.asList(resultSetColumnDescriptors), rows);
	}

	

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
}
