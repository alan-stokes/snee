package uk.ac.manchester.cs.snee.datasource.webservice;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.ggf.namespaces._2005._12.ws_dair.SQLDatasetType;
import org.ggf.namespaces._2005._12.ws_dair.SQLExecuteRequest;
import org.ggf.namespaces._2005._12.ws_dair.SQLExecuteResponse;
import org.ggf.namespaces._2005._12.ws_dair.SQLExpressionType;
import org.ggf.namespaces._2005._12.ws_dair.SQLPropertyDocumentType;
import org.ggf.namespaces._2005._12.ws_dair.SchemaDescription;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import eu.semsorgrid4env.service.wsdai.DataResourceAddressType;
import eu.semsorgrid4env.service.wsdai.DataResourceUnavailableFault;
import eu.semsorgrid4env.service.wsdai.GetDataResourcePropertyDocumentRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListResponse;
import eu.semsorgrid4env.service.wsdai.InvalidResourceNameFault;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.PropertyDocumentType;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;

public class WSDAIRSourceWrapper extends SourceWrapperAbstract {

	private static Logger logger = 
		Logger.getLogger(WSDAIRSourceWrapper.class.getName());
	
	private static String _languageURI =
		"http://www.sqlquery.org/sql-92";

	private WSDAIRAccessServiceClient _wsdairClient;

    public WSDAIRSourceWrapper(String url, Types types) 
    throws MalformedURLException {
    	super(url, types);
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER WSDAIRSourceWrapper() with URL " + url);
    	_wsdairClient = createServiceClient(url);
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
    		List<DataResourceAddressType> addresses = response.getDataResourceAddress();
    		for (DataResourceAddressType address : addresses) {
    			resourceNames.add(address.getAddress().getValue());
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
    	//XXX: Assumes that a source only provides a single extent
    	List<ExtentMetadata> extents = null;
		try {
			PropertyDocumentType propDoc = 
				getPropertyDocument(resourceName);
    		/*
    		 * Convert the property document to a 
    		 * sql property document
    		 */
    		SQLPropertyDocumentType wsdairPropDoc = 
    			(SQLPropertyDocumentType) propDoc;
    		SchemaDescription schemaDescription = 
    			wsdairPropDoc.getSchemaDescription();
    		if (schemaDescription != null) {
    			String schemaDoc = 
    				(String) schemaDescription.getAny().get(0);
    			SchemaParser schemaParser = 
    				new OgsadaiSchemaParser(schemaDoc, _types);
    			extents  = extractSchema(schemaParser, ExtentType.TABLE);
    		}
    	} catch (ClassCastException e) {
    		String msg = "Unable to construct a sql property " +
    				"document from the received property document.";
    		logger.warn(msg);
    		throw new SchemaMetadataException(msg);
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

    private PropertyDocumentType getPropertyDocument(
    		String resourceName)
    throws SchemaMetadataException, InvalidResourceNameFault,
    DataResourceUnavailableFault, NotAuthorizedFault, 
    ServiceBusyFault {
    	if (logger.isTraceEnabled()) {
    		logger.trace("ENTER getPropertyDocument() with " + 
    				resourceName);
    	}
    	GetDataResourcePropertyDocumentRequest request = 
    		new GetDataResourcePropertyDocumentRequest();
    	request.setDataResourceAbstractName(resourceName);
    	PropertyDocumentType propDoc = 
    		_wsdairClient.getPropertyDocument(request);
    	if (logger.isTraceEnabled()) {
    		logger.trace("RETURN getPropertyDocument() with " + 
    				propDoc.getDataResourceAbstractName());
    	}
    	return propDoc;
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
		request.setDatasetFormatURI(DATASET_FORMAT);
		SQLExpressionType sqlExpression = new SQLExpressionType();
		sqlExpression.setExpression(query);
		sqlExpression.setLanguage(_languageURI);
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
		if (formatURI.trim().equalsIgnoreCase(DATASET_FORMAT)) {
			if (logger.isTraceEnabled()) {
				logger.trace("Processing dataset with format " +
						formatURI);
			}
			List<Object> datasets = 
				sqlDataset.getDatasetData().getContent();
			if (logger.isTraceEnabled()) {
				logger.trace("Number of datasets: " + datasets.size());
			}
			for (Object data : datasets) {
				tuples.addAll(processWRSDataset((String) data));
			}
		} else {
			String msg = "Unknown dataset format URI " + formatURI + 
				". Unable to process dataset.";
			logger.warn(msg);
			throw new SNEEDataSourceException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processSQLQueryResponse() with " +
					tuples );
		return tuples;
	}
    
}