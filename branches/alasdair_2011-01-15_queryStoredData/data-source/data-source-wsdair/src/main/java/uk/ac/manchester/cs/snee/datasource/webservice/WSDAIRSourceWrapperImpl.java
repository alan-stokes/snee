package uk.ac.manchester.cs.snee.datasource.webservice;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cxf.ws.addressing.ReferenceParametersType;
import org.apache.log4j.Logger;
import org.ggf.namespaces._2005._12.ws_dai.DataResourceAddressType;
import org.ggf.namespaces._2005._12.ws_dai.DataResourceUnavailableFault;
import org.ggf.namespaces._2005._12.ws_dai.GetDataResourcePropertyDocumentRequest;
import org.ggf.namespaces._2005._12.ws_dai.GetResourceListRequest;
import org.ggf.namespaces._2005._12.ws_dai.GetResourceListResponse;
import org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFault;
import org.ggf.namespaces._2005._12.ws_dai.NotAuthorizedFault;
import org.ggf.namespaces._2005._12.ws_dai.ServiceBusyFault;
import org.ggf.namespaces._2005._12.ws_dair.SQLDatasetType;
import org.ggf.namespaces._2005._12.ws_dair.SQLExecuteRequest;
import org.ggf.namespaces._2005._12.ws_dair.SQLExecuteResponse;
import org.ggf.namespaces._2005._12.ws_dair.SQLExpressionType;
import org.ggf.namespaces._2005._12.ws_dair.SQLPropertyDocumentType;
import org.ggf.namespaces._2005._12.ws_dair.SchemaDescription;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.sun.java.xml.ns.jdbc.Data;
import com.sun.java.xml.ns.jdbc.Data.CurrentRow;
import com.sun.java.xml.ns.jdbc.Metadata;
import com.sun.java.xml.ns.jdbc.Metadata.ColumnDefinition;
import com.sun.java.xml.ns.jdbc.WebRowSet;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class WSDAIRSourceWrapperImpl extends SourceWrapperAbstract 
implements SourceWrapper {

	private static Logger logger = 
		Logger.getLogger(WSDAIRSourceWrapperImpl.class.getName());
	
	private static String SQL_LANGUAGE_URI =
		"http://www.sqlquery.org/sql-92";
	
	//XXX: WSDAIR fails with this format
	private static String CSV_DATASET_FORMAT = 
		"http://ogsadai.org.uk/data/csv";
	
	//XXX: OGSA-DAI do not use the standard WRS object!
	private static String WRS_DATASET_FORMAT =
		"http://java.sun.com/xml/ns/jdbc";
		
//
//	private static String WSDAI_NS =
//		"http://www.ggf.org/namespaces/2005/12/WS-DAI/";
	
	private WSDAIRAccessServiceClient _wsdairClient;

	private SQLUtils _sqlUtils;

    public WSDAIRSourceWrapperImpl(String url, Types types) 
    throws MalformedURLException {
    	super(url, types);
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER WSDAIRSourceWrapper() with URL " + url);
    	_wsdairClient = createServiceClient(url);
    	_sourceType = SourceType.WSDAIR;
    	_sqlUtils = new SQLUtils(types);
        if (logger.isDebugEnabled())
        	logger.debug("RETURN WSDAIRSourceWrapper()");
    }

	protected WSDAIRAccessServiceClient createServiceClient(String url) 
    throws MalformedURLException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createServiceClient() with " + url);
		WSDAIRAccessServiceClient wsdairClient = 
			new WSDAIRAccessServiceClient(url);
		if (logger.isTraceEnabled())
			logger.trace("RETURN createServiceClient()");
		return wsdairClient;
	}
    
    public List<String> getResourceNames() 
    throws SNEEDataSourceException 
    {
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER getResourceNames()");
    	GetResourceListRequest request = new GetResourceListRequest();
    	GetResourceListResponse response;
    	List<String> resourceNames = new ArrayList<String>();
    	try {
    		response = _wsdairClient.getResourceList(request);
    		List<DataResourceAddressType> addresses = 
    			response.getDataResourceAddress();
    		String nameLog = "";
    		if (logger.isTraceEnabled()) {
    			logger.trace("Number of addresses: " + addresses.size());
    		}
    		for (DataResourceAddressType address : addresses) {
    			ReferenceParametersType refParams = 
    				address.getReferenceParameters();
    			Node xmlNode = (Node) refParams.getAny().get(0);
    			String resourceName = xmlNode.getTextContent();
    			resourceNames.add(resourceName);
				nameLog += resourceName + ", ";
    		}
    		if (logger.isTraceEnabled()) {
    			logger.trace("Resource Names: " + 
    					nameLog.substring(0, nameLog.lastIndexOf(",")));
    		}
    	} catch (NotAuthorizedFault e) {
    		String msg = "Not authorized to access service.";
    		logger.warn(msg, e);
    		throw new SNEEDataSourceException(msg, e);
    	} catch (ServiceBusyFault e) {
    		String msg = "Service currently busy.";
    		logger.warn(msg, e);
    		throw new SNEEDataSourceException(msg, e);
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
	    	GetDataResourcePropertyDocumentRequest request = 
	    		new GetDataResourcePropertyDocumentRequest();
	    	request.setDataResourceAbstractName(resourceName);
	    	SQLPropertyDocumentType propDoc = 
	    		_wsdairClient.getSQLPropertyDocument(request);
			if (logger.isTraceEnabled()) {
				logger.trace("Retrieving Schema element");
			}
			SchemaDescription schemaDescription = 
				propDoc.getSchemaDescription();
			if (schemaDescription != null) {					
				Element schemaDoc = 
					(Element) schemaDescription.getAny().get(0);
				SchemaParser schemaParser = 
					new OgsadaiSchemaParser(schemaDoc, _types);
				extents  = extractSchema(schemaParser, ExtentType.TABLE);
			}
		} catch (SNEEDataSourceException e) {
			if (!e.getMessage().equals("Wrong service type")) {
				throw e;
			}
    	} catch (InvalidResourceNameFault e) {
    		String msg = "Resource name " + resourceName + 
    			" unknown to service " + _url + ".";
    		logger.warn(msg, e);
    		throw new SNEEDataSourceException(msg, e);
		} catch (DataResourceUnavailableFault e) {
    		String msg = "Resource " + resourceName + 
    			" currently unavailable on " + _url + ".";
    		logger.warn(msg, e);
    		throw new SNEEDataSourceException(msg, e);
		} catch (NotAuthorizedFault e) {
    		String msg = "Not authorised to use service " + _url + ".";
    		logger.warn(msg, e);
    		throw new SNEEDataSourceException(msg, e);
		} catch (ServiceBusyFault e) {
    		String msg = "Service " + _url + " is busy.";
    		logger.warn(msg, e);
    		throw new SNEEDataSourceException(msg, e);
		}
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
		SQLExecuteRequest request = new SQLExecuteRequest();
		request.setDataResourceAbstractName(resourceName);
		request.setDatasetFormatURI(WRS_DATASET_FORMAT);
		SQLExpressionType sqlExpression = new SQLExpressionType();
		sqlExpression.setExpression(query);
		sqlExpression.setLanguage(SQL_LANGUAGE_URI);
		request.setSQLExpression(sqlExpression);
		SQLExecuteResponse response = _wsdairClient.sqlExecute(request);
		List <Tuple> tuples = processSQLQueryResponse(response);
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
	private List<Tuple> processSQLQueryResponse(
			SQLExecuteResponse response) 
	throws SNEEDataSourceException, TypeMappingException, 
	SchemaMetadataException, SNEEException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processSQLQueryResponse() with " +
					response);
		List<Tuple> tuples = new ArrayList<Tuple>();
		SQLDatasetType sqlDataset = response.getSQLDataset();
		String formatURI = sqlDataset.getDatasetFormatURI();
		List<Object> datasets = 
			sqlDataset.getDatasetData().getContent();
		if (logger.isTraceEnabled()) {
			logger.trace("Number of datasets: " + datasets.size());
		}
		for (Object data : datasets) {
			if (formatURI.trim().equalsIgnoreCase(CSV_DATASET_FORMAT)) {
				if (logger.isTraceEnabled()) {
					logger.trace("Processing CSV dataset");
				}
				//FIXME: Process csv
				String msg = "CSV format not currently processed.";
				logger.warn(msg);
				throw new SNEEDataSourceException(msg);
			} else if (formatURI.trim().equalsIgnoreCase(WRS_DATASET_FORMAT)) {
				if (logger.isTraceEnabled()) {
					logger.trace("Processing WRS dataset");
				}
				tuples.addAll(processWRSDataset((WebRowSet) data));
			} else {
				String msg = "Unknown dataset format URI " + formatURI + 
				". Unable to process dataset.";
				logger.warn(msg);
				throw new SNEEDataSourceException(msg);
			}
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processSQLQueryResponse() with " +
					tuples );
		return tuples;
	}
    
	protected List<Tuple> processWRSDataset(WebRowSet wrs)
	throws TypeMappingException, SchemaMetadataException,
			SNEEDataSourceException, SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processWRSDataset(WSDAIR WebRowSet) with " + 
					wrs);
		}
		List<Tuple> tuples = new ArrayList<Tuple>();
		Metadata metadata = wrs.getMetadata();
		Data data = wrs.getData();
		List<ColumnDefinition> columns = metadata.getColumnDefinition();
		int numCols = columns.size();
		logger.debug("Number of columns: " + numCols);
		int count = 0;
		String[] attrNames = new String[numCols];
		String[] extentNames = new String[numCols];
		AttributeType[] dataTypes = new AttributeType[numCols];
		for (ColumnDefinition colDef : columns) {
			attrNames[count] = colDef.getColumnLabel();
			extentNames[count] = colDef.getTableName();
			int columnType = new Integer(colDef.getColumnType()).intValue();
			dataTypes[count] = _sqlUtils.inferType(columnType);
			count++;
		}
		List<Object> rows = data.getCurrentRowAndInsertRowAndDeleteRow();
		int numberRows = rows.size();
		logger.debug("Number of rows: " + numberRows);
		for (Object rowObj : rows) {
			if (rowObj instanceof CurrentRow) {
				//Create tuple
				Tuple tuple = new Tuple();
				CurrentRow row = (CurrentRow) rowObj;
				List<Object> rowColumns = row.getColumnValue();
				for (int i = 0; i < numCols; i++) {
					Element colNode = (Element) rowColumns.get(i);
					String colValue = colNode.getTextContent();
					Object value = 
						convertStringToType(colValue, dataTypes[i].getName());
					EvaluatorAttribute attr = 
						new EvaluatorAttribute(extentNames[i], attrNames[i], 
							dataTypes[i], value);
//					if (logger.isTraceEnabled()) {
//						logger.trace("Received attribute: " + attr);
//					}
					tuple.addAttribute(attr);
				}
				tuples.add(tuple);
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processWRSDataset(WSDAIR WebRowSet), " +
					"number of tuples " + tuples.size());
		}
		return tuples;
	}

	private Object convertStringToType(String colValue,
			String typeName) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER convertStringToType() with " +
					colValue + " " + typeName);
		}
		Object obj;
		if (colValue == null || colValue.isEmpty() || colValue.equals("")) {
			logger.debug("Found a NULL Value. Representing with java.sql.Types.NULL");
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