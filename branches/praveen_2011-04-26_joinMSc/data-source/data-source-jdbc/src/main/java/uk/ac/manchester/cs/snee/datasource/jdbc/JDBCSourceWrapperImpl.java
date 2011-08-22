package uk.ac.manchester.cs.snee.datasource.jdbc;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.datasource.webservice.SQLUtils;
import uk.ac.manchester.cs.snee.datasource.webservice.SourceWrapper;
import uk.ac.manchester.cs.snee.datasource.webservice.StoredSourceWrapperAbstract;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class JDBCSourceWrapperImpl extends StoredSourceWrapperAbstract 
implements SourceWrapper {

	private static Logger logger = 
		Logger.getLogger(JDBCSourceWrapperImpl.class.getName());

	private SQLUtils _sqlUtils;

	private Connection _dbConn;

    public JDBCSourceWrapperImpl(String url, Types types) 
    throws MalformedURLException, SNEEDataSourceException {
    	super(url, types);
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER JDBCSourceWrapper() with URL " + url);
    	_sourceType = SourceType.RELATIONAL;
    	_sqlUtils = new SQLUtils(types);
    	try {
			_dbConn = connectToDatabase(url);
		} catch (SQLException e) {
			String message = "Problem connecting to database " + url;
			logger.warn(message, e);
			throw new SNEEDataSourceException(message, e);
		}
        if (logger.isDebugEnabled())
        	logger.debug("RETURN JDBCSourceWrapper()");
    }
    
    private Connection connectToDatabase(String url) 
    throws SQLException 
    {
    	if (logger.isTraceEnabled()) {
    		logger.trace("ENTER connectToDatabase() with " + url);
    	}
    	Connection conn = DriverManager.getConnection(url);
    	if (logger.isInfoEnabled()) {
    		logger.info("Connected to database " + url);
    	}
    	
    	return conn;
	}

	public List<String> getResourceNames() 
    throws SNEEDataSourceException 
    {
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER getResourceNames()");
    	List<String> resourceNames = new ArrayList<String>();
    	try {
			String catalog = _dbConn.getCatalog();
			resourceNames.add(catalog);
		} catch (SQLException e) {
			logger.warn(e);
			throw new SNEEDataSourceException(e);
		}
    	if (logger.isDebugEnabled())
			logger.debug("RETURN getResourceNames(), " +
					"number of resources " + resourceNames.size());
		return resourceNames;
	}

	public List<ExtentMetadata> getSchema(String resourceName) 
    throws SNEEDataSourceException, SchemaMetadataException, 
    TypeMappingException {
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER getSchema() with " + resourceName);
    	List<ExtentMetadata> extents = new ArrayList<ExtentMetadata>();
    	try {
    		DatabaseMetaData dbmd = _dbConn.getMetaData();
    		List<String> tableNames = getTableNames(dbmd);
    		for (String tableName : tableNames) {
    			extents.add(getExtentMetadata(dbmd, tableName));
    		}
    	} catch (SQLException e) {
    		throw new SNEEDataSourceException(e);
    	}
    	if (logger.isDebugEnabled())
    		logger.debug("RETURN getSchema() with " + extents);
    	return extents;
    }

	private List<String> getTableNames(DatabaseMetaData dbmd) 
	throws SQLException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getTableNames()");
		}
		String[] types = { "TABLE" };
		ResultSet resultSet = dbmd.getTables(null, null, "%", types);
		List<String> tableNames = new ArrayList<String>();
		while (resultSet.next()) {
			String tableName = resultSet.getString(3);
			tableNames.add(tableName);
		}
		resultSet.close();
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getTableNames() #tables=" + tableNames.size());
		}
		return tableNames;
	}

	private ExtentMetadata getExtentMetadata(DatabaseMetaData dbmd, String tableName) 
	throws SQLException, TypeMappingException, SchemaMetadataException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getExtentMetadata() for " + tableName);
		}
		List<Attribute> attributes = getAttributes(dbmd, tableName);
		ExtentMetadata extent = new ExtentMetadata(tableName.toLowerCase(), 
				attributes, ExtentType.TABLE); 
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getExtentMetadata()" + extent);
		}
		return extent;
	}

	private List<Attribute> getAttributes(DatabaseMetaData dbmd, String tableName) 
	throws SQLException, TypeMappingException, SchemaMetadataException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getAttributes() for " + tableName);
		}
		List<Attribute> attributes = new ArrayList<Attribute>();
		ResultSet rsColumns = dbmd.getColumns(null, null, tableName, null);
	    while (rsColumns.next()) {
	      String attrName = rsColumns.getString("COLUMN_NAME");
	      AttributeType attrType = _sqlUtils.inferType(rsColumns.getInt("DATA_TYPE"));
	      Attribute attribute = new DataAttribute(tableName, attrName, attrType);
	      attributes.add(attribute);
	    }		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getAttributes() #tables=" + attributes.size());
		}
		return attributes;
	}

	public List<Tuple> executeQuery(String resourceName, String query) 
	throws SNEEDataSourceException, TypeMappingException,
	SchemaMetadataException, SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER executeQuery() on resource " +
					resourceName + " query " + query);
		}
		ResultSet results;
		Statement stmt = null;
		List<Tuple> tuples = new ArrayList<Tuple>();
		try {
			stmt = _dbConn.createStatement();
			results = stmt.executeQuery(query);
			tuples = processSQLQueryResponse(results);
		} catch (SQLException e) {
			String message = "Problem posing query to database " + query;
			logger.warn(message, e);
			throw new SNEEDataSourceException(message, e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.warn(e);
					throw new SNEEDataSourceException(e);
				}
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN executeQuery() #tuples=" + tuples.size());
		}
		return tuples;
	}
	
	/**
	 * Processes the response from the service to convert the 
	 * WebRowSet into tuples for the SNEE stream evaluation engine
	 * @param rs the answer document from the service containing a WebRowSet
	 * @return list of tuples that can be used by the SNEE evaluation engine
	 * @throws SNEEDataSourceException Error processing data from data source
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 * @throws SNEEException 
	 * @throws SNEEException 
	 */
	private List<Tuple> processSQLQueryResponse(ResultSet rs) 
	throws SNEEDataSourceException, TypeMappingException, 
	SchemaMetadataException, SNEEException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processSQLQueryResponse() with " + rs);
		List<Tuple> tuples = new ArrayList<Tuple>();
		try {
			ResultSetMetaData rsMetadata = rs.getMetaData();
			int columnCount = rsMetadata.getColumnCount();
			String[] columnNames = new String[columnCount];
			AttributeType[] attrTypes = new AttributeType[columnCount];
			for (int i = 0; i < columnCount; i++) {
				columnNames[i] = rsMetadata.getColumnName(i+1);
				int sqlType = rsMetadata.getColumnType(i+1);
				attrTypes[i] = _sqlUtils.inferType(sqlType);
			}
			String extentName = rsMetadata.getTableName(1);
			while (rs.next()) {
				List<EvaluatorAttribute> attrValues = new ArrayList<EvaluatorAttribute>();
				for (int i = 0; i < columnCount; i++) {
					EvaluatorAttribute attr = 
						new EvaluatorAttribute(extentName, columnNames[i], 
								attrTypes[i], rs.getObject(i+1));
					attrValues.add(attr);
				}
				tuples.add(new Tuple(attrValues));
			}
		} catch (SQLException e) {
			String message = "Problem processing result set";
			logger.warn(message, e);
			throw new SNEEDataSourceException(message, e);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processSQLQueryResponse() with #tuples" + 
					tuples.size());
		return tuples;
	}

	public List<Tuple> getData(String resourceName)
	throws SNEEDataSourceException, TypeMappingException, 
	SchemaMetadataException, SNEEException
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getData() with " + resourceName);
		}
		List<Tuple> tuples = new ArrayList<Tuple>();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getData() #tuples " + tuples.size());
		}
		return tuples;
	}
}