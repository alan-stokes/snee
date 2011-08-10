package uk.ac.manchester.cs.snee.datasource.jdbc;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.datasource.webservice.SQLUtils;
import uk.ac.manchester.cs.snee.datasource.webservice.SourceWrapper;
import uk.ac.manchester.cs.snee.datasource.webservice.SourceWrapperAbstract;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class JDBCSourceWrapperImpl extends SourceWrapperAbstract 
implements SourceWrapper {

	private static Logger logger = 
		Logger.getLogger(JDBCSourceWrapperImpl.class.getName());

	private SQLUtils _sqlUtils;

    public JDBCSourceWrapperImpl(String url, Types types) 
    throws MalformedURLException {
    	super(url, types);
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER JDBCSourceWrapper() with URL " + url);
    	_sourceType = SourceType.RELATIONAL;
    	_sqlUtils = new SQLUtils(types);
        if (logger.isDebugEnabled())
        	logger.debug("RETURN JDBCSourceWrapper()");
    }
    
    public List<String> getResourceNames() 
    throws SNEEDataSourceException 
    {
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER getResourceNames()");
    	//FIXME: return the name of the available database
    	List<String> resourceNames = new ArrayList<String>();
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
    	//FIXME: return extent metadata extracted from the database
    	if (logger.isDebugEnabled())
    		logger.debug("RETURN getSchema() with " + extents);
    	return extents;
    }

	public List<Tuple> executeQuery(String resourceName, String query) 
	throws SNEEDataSourceException, TypeMappingException,
	SchemaMetadataException, SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER executeQuery() on resource " +
					resourceName + " query " + query);
		}
		//FIXME: execute query over database. Process result set into a list of tuples
		ResultSet results = null;
		List<Tuple> tuples = processSQLQueryResponse(results);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN executeQuery()");
		}
		return tuples;
	}
	
	/**
	 * Processes the response from the service to convert the 
	 * WebRowSet into tuples for the SNEE stream evaluation engine
	 * @param response the answer document from the service containing a WebRowSet
	 * @return list of tuples that can be used by the SNEE evaluation engine
	 * @throws SNEEDataSourceException Error processing data from data source
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 * @throws SNEEException 
	 * @throws SNEEException 
	 */
	private List<Tuple> processSQLQueryResponse(ResultSet response) 
	throws SNEEDataSourceException, TypeMappingException, 
	SchemaMetadataException, SNEEException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processSQLQueryResponse() with " +
					response);
		List<Tuple> tuples = new ArrayList<Tuple>();
		//FIXME: process the result set
		if (logger.isTraceEnabled())
			logger.trace("RETURN processSQLQueryResponse() with " +
					tuples );
		return tuples;
	}

	//XXX if you need this method then move to a common super class
	private Object convertStringToType(String colValue,
			String typeName) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER convertStringToType() with " +
					colValue + " " + typeName);
		}
		Object obj;
		if (colValue == null || colValue.isEmpty() || colValue.equals("")) {
			logger.trace("Found a NULL Value. Representing with java.sql.Types.NULL");
			obj = java.sql.Types.NULL;
		} else if (typeName.equals("boolean")) {
			obj = new Boolean(colValue);
		} else if (typeName.equals("decimal")) {
				obj = new BigDecimal(colValue);
		} else if (typeName.equals("float")) {
			obj = new Float(colValue);
		} else if (typeName.equals("integer")) {
			obj = new Integer(colValue);
		} else if (typeName.equals("string")) {
			obj = colValue;
		} else if (typeName.equals("timestamp")) {
			obj = new Long(colValue);
		} else {
			obj = colValue;
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN convertStringToType() with " + obj);
		}
		return obj;
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