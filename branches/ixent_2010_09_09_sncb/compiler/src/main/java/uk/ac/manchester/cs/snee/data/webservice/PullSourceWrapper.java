package uk.ac.manchester.cs.snee.data.webservice;

import java.io.StringReader;
import java.net.MalformedURLException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.WebRowSet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.evaluator.types.Field;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

import com.sun.rowset.WebRowSetImpl;

import eu.semsorgrid4env.service.stream.StreamDescriptionType;
import eu.semsorgrid4env.service.stream.pull.GetStreamItemRequest;
import eu.semsorgrid4env.service.stream.pull.GetStreamNewestItemRequest;
import eu.semsorgrid4env.service.stream.pull.PullStreamPropertyDocumentType;
import eu.semsorgrid4env.service.wsdai.DataResourceAddressType;
import eu.semsorgrid4env.service.wsdai.DataResourceUnavailableFault;
import eu.semsorgrid4env.service.wsdai.DatasetType;
import eu.semsorgrid4env.service.wsdai.GenericQueryResponse;
import eu.semsorgrid4env.service.wsdai.GetDataResourcePropertyDocumentRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListResponse;
import eu.semsorgrid4env.service.wsdai.InvalidResourceNameFault;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.PropertyDocumentType;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;
public class PullSourceWrapper {

	private static Logger logger = 
		Logger.getLogger(PullSourceWrapper.class.getName());

	private DateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	private Types _types;

	private PullStreamServiceClient _pullClient;
	
	private static String _datasetFormatURI =
		"http://java.sun.com/xml/ns/jdbc";
	
	private int _maxTuples = 10;

	private String _url;

    public PullSourceWrapper(String url, Types types) 
    throws MalformedURLException {
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER PullSourceWrapper() with URL " + url);
    	_types = types;
    	_pullClient = createServiceClient(url);
    	_url = url;
        if (logger.isDebugEnabled())
        	logger.debug("RETURN PullSourceWrapper()");
    }
    
    /**
     * Constructor for testing purposes only!
     * @param client
     */
    protected PullSourceWrapper(PullStreamServiceClient client, Types types) {
		// Constructor for testing purposes only
    	_types = types;
    	_pullClient = client;
	}

	protected PullStreamServiceClient createServiceClient(String url) 
    throws MalformedURLException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createServiceClient() with " + url);
		PullStreamServiceClient pullClient = new PullStreamServiceClient(url);
		if (logger.isTraceEnabled())
			logger.trace("RETURN createServiceClient()");
		return pullClient;
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
    		response = _pullClient.getResourceList(request);
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

	public ExtentMetadata getSchema(String resourceName) 
    throws SNEEDataSourceException, SchemaMetadataException, 
    TypeMappingException {
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER getSchema() with " + resourceName);
    	//XXX: Assumes that a source only provides a single extent
    	ExtentMetadata extentMetadata = null;
		try {
			PropertyDocumentType propDoc = getPropertyDocument(resourceName);
    		// Convert the property document to a pull stream property document
    		PullStreamPropertyDocumentType pullStreamPropDoc = 
    			(PullStreamPropertyDocumentType) propDoc;
    		StreamDescriptionType streamDesciption = pullStreamPropDoc.getStreamDescription();
    		if (streamDesciption != null) {
    			extentMetadata  = extractSchema(streamDesciption);
    		}
    	} catch (ClassCastException e) {
    		String msg = "Unable to construct a pull stream property document from the received property document.";
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
    		logger.debug("RETURN getSchema() with " + extentMetadata);
    	return extentMetadata;
    }

    private PropertyDocumentType getPropertyDocument(String resourceName)
    throws SchemaMetadataException, InvalidResourceNameFault,
    DataResourceUnavailableFault, NotAuthorizedFault, ServiceBusyFault {
    	if (logger.isTraceEnabled())
    		logger.trace("ENTER getPropertyDocument() with " + resourceName);
    	/*
    	 * This method is currently assuming that the CCO-WS will
    	 * return the schema description for the stream as part of
    	 * the property document. The schema will be expressed using
    	 * the ogsa-dai specification.
    	 */

    	GetDataResourcePropertyDocumentRequest request = new GetDataResourcePropertyDocumentRequest();
    	request.setDataResourceAbstractName(resourceName);
    	PropertyDocumentType propDoc = _pullClient.getPropertyDocument(request);
    	if (logger.isTraceEnabled())
    		logger.trace("RETURN getPropertyDocument() with " + propDoc.getDataResourceAbstractName());
    	return propDoc;
    }

	private ExtentMetadata extractSchema(
			StreamDescriptionType streamDesciption) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER extractSchema()");
		ExtentMetadata extentMetadata;
		String schemaFormatURI = 
			streamDesciption.getStreamDescriptionFormatURI();
		//Extract the schema element from the property document
		String schemaDoc = 
			(String) streamDesciption.getStreamDescriptionDocument();
		SchemaParser schemaParser;
		if (schemaFormatURI.equals("http://ogsadai.org.uk/namespaces/2005/10/properties")) {
			schemaParser = new OgsadaiSchemaParser(schemaDoc, _types);
		} else if (schemaFormatURI.equals("http://java.sun.com/xml/ns/jdbc")) {
			schemaParser = new WrsSchemaParser(schemaDoc, _types);
		} else {
			String msg = "Unknown schema definition format " + schemaFormatURI;
			logger.warn(msg);
			throw new SchemaMetadataException(msg);
		}
		String extentName = schemaParser.getExtentName();
		Map<String, AttributeType> attributes = schemaParser.getColumns();
		extentMetadata  = new ExtentMetadata(extentName, attributes, 
				ExtentType.PUSHED);
		if (logger.isTraceEnabled())
			logger.trace("RETURN extractSchema() with " + extentMetadata);
		return extentMetadata;
	}

	/**
	 * Retrieves stream items from the supplied resource name, starting
	 * with the oldest available or the last seen stream item and 
	 * moving forward in time. The resource will only request up to
	 * the maximum number of tuples.
	 * @param resourceName
	 * @return
	 * @throws SNEEDataSourceException
	 * @throws TypeMappingException
	 * @throws SchemaMetadataException
	 * @throws SNEEException
	 */
	public List<Tuple> getData(String resourceName) 
	throws SNEEDataSourceException, TypeMappingException, 
	SchemaMetadataException, SNEEException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getData() with " + resourceName);
		GetStreamItemRequest request = 
			createGetStreamItemRequest(resourceName, null, null);
		GenericQueryResponse response = _pullClient.getStreamItems(request);;
		List<Tuple> tuples = processGenericQueryResponse(response);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getData() #tuples=" + tuples.size());
		return tuples;
	}

	public List<Tuple> getData(String resourceName, int numItems, 
			Timestamp timestamp) 
	throws SNEEDataSourceException, TypeMappingException,
	SchemaMetadataException, SNEEException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getData() with " + resourceName + 
					" #items=" + numItems +
					" timestamp=" + timestamp);
		GetStreamItemRequest request = 
			createGetStreamItemRequest(resourceName, numItems, timestamp);
			GenericQueryResponse response = _pullClient.getStreamItems(request);
			List<Tuple> tuples = processGenericQueryResponse(response);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getData() #tuples=" + tuples.size());
		return tuples;
	}

	/**
	 * Creates the request document for the GetStreamItem operation
	 * call
	 * @param resourceName the name of the resource for which data is to be retrieved
	 * @param numItems number of items to be retrieved
	 * @param timestamp optional if items are to be returned from a specified time point forward
	 * @return request document
	 */
	private GetStreamItemRequest createGetStreamItemRequest(
			String resourceName, Integer numItems, Timestamp timestamp) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createGetStreamItemRequest() with " +
					resourceName +
					" #items=" + numItems +
					" timestamp=" + timestamp);
		GetStreamItemRequest request = new GetStreamItemRequest();
		request.setDataResourceAbstractName(resourceName);
		request.setDatasetFormatURI(_datasetFormatURI);
		if (numItems != null) {
			request.setCount(numItems.toString());
		}
		if (timestamp != null) {
			long timeMillis = timestamp.getTime();
			request.setPosition(dateFormat.format(new Date(timeMillis)));
		}
		request.setMaximumTuples(_maxTuples);
		if (logger.isTraceEnabled())
			logger.trace("RETURN createGetStreamItemRequest() with " +
					request);
		return request;
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
	private List<Tuple> processGenericQueryResponse(
			GenericQueryResponse response) 
	throws SNEEDataSourceException, TypeMappingException, 
	SchemaMetadataException, SNEEException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processGenericQueryResponse() with " +
					response);
		List<Tuple> tuples = new ArrayList<Tuple>();
		DatasetType dataset = response.getDataset();
		String formatURI = dataset.getDatasetFormatURI();
		if (formatURI.trim().equalsIgnoreCase(_datasetFormatURI)) {
			if (logger.isTraceEnabled())
				logger.trace("Processing dataset with format " + formatURI);
			List<Object> datasets = dataset.getDatasetData().getContent();
			if (logger.isTraceEnabled()) {
				logger.trace("Number of datasets: " + datasets.size());
			}
			for (Object data : datasets) {
				tuples.addAll(processDataset((String) data));
			}
		} else {
			String msg = "Unknown dataset format URI " + formatURI + 
				". Unable to process dataset.";
			logger.warn(msg);
			throw new SNEEDataSourceException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processGenericResponse() with " +
					tuples );
		return tuples;
	}

	private List<Tuple> processDataset(String data)
	throws TypeMappingException, SchemaMetadataException,
			SNEEDataSourceException, SNEEException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processDataset() with " + data);
		List<Tuple> tuples = new ArrayList<Tuple>();
		try {
			WebRowSet wrs = new WebRowSetImpl();
			wrs.readXml(new StringReader(data));
			if (logger.isTraceEnabled())
				logger.trace("Successfully read in WebRowSet. " +
						"Number of rows " + wrs.size());
			//Retrieve resultset metadata
			ResultSetMetaData wrsMetadata = wrs.getMetaData();
			int numberColumns = wrsMetadata.getColumnCount();
			//Move pointer to before first tuple
			wrs.beforeFirst();
			//Loop through webrowset
			while (wrs.next()) {
				//Retrieve the next row from the webrowset
				int rowNumber = wrs.getRow();
				//Create tuple
				Tuple tuple = new Tuple();
				for (int i = 1; i <= numberColumns; i++) {
					//Populate fields of tuple
					String name = wrsMetadata.getColumnLabel(i);
					AttributeType dataType = inferType(wrsMetadata, i);
					Object value = wrs.getObject(i);
					Field field = new Field(name, dataType, value);
					tuple.addField(field);
				}
				//Add tuple to tuple set
				tuples.add(tuple);
			}
		} catch (SQLException e) {
			String msg = "Problem reading in WebRowSet. " + e;
			logger.warn(msg);
			throw new SNEEDataSourceException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processDataset(), number of tuples " + 
					tuples.size());
		return tuples;
	}

	private AttributeType inferType(ResultSetMetaData wrsMetadata, int colIndex) 
	throws SQLException, TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER inferType() with column: " + colIndex);
		AttributeType dataType;
		int colType = wrsMetadata.getColumnType(colIndex);
		if (logger.isTraceEnabled())
			logger.trace("Column type code: " + colType);
		switch (colType) {
		case java.sql.Types.CHAR:
			dataType = _types.getType("string");
			break;
		case java.sql.Types.DECIMAL:
		case java.sql.Types.REAL:
		case java.sql.Types.FLOAT:
			dataType = _types.getType("float");
			break;
		case java.sql.Types.INTEGER:
			//Corresponds to INT or INTEGER
			dataType = _types.getType("integer");
			break;
		case java.sql.Types.TIMESTAMP:
			dataType = _types.getType("timestamp");
			break;
//		case 93:
//			//Corresponds to DATETIME
//			dataType = _types.getType("string");
//			break;
		default:
			String msg = "Unsupported data type " + 
				wrsMetadata.getColumnTypeName(colIndex);
			logger.warn(msg);
			throw new SchemaMetadataException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN inferType() with " + dataType);
		return dataType;
	}

	public List<Tuple> getNewestData(String resourceName, Integer numItems) 
	throws SNEEDataSourceException, TypeMappingException, 
	SchemaMetadataException, SNEEException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getNewestData() with " + resourceName +
					" #items=" + numItems);
		GetStreamNewestItemRequest request = 
			new GetStreamNewestItemRequest();
		request.setDataResourceAbstractName(resourceName);
		request.setDatasetFormatURI(_datasetFormatURI);
		if (numItems != null) {
			request.setCount(numItems.toString());
		}
		request.setMaximumTuples(_maxTuples);
		GenericQueryResponse response = _pullClient.getStreamNewestItem(request);;
		List<Tuple> tuples = processGenericQueryResponse(response);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getData() #tuples=" + tuples.size());
		return tuples;
	}
    
}
