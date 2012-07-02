package uk.ac.manchester.cs.snee.datasource.webservice;

import java.net.MalformedURLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import eu.semsorgrid4env.service.stream.StreamDescriptionType;
import eu.semsorgrid4env.service.stream.StreamRateType;
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

public class PullSourceWrapperImpl extends SourceWrapperAbstract
implements PullSourceWrapper {

	private static final Logger logger = Logger.getLogger(PullSourceWrapperImpl.class.getName());

	private DateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
	private int _maxTuples = 10;

	private PullStreamServiceClient _pullClient;

    public PullSourceWrapperImpl(String url, Types types) 
    throws MalformedURLException {
    	super(url, types);
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER PullSourceWrapper() with URL " + url);
    	_pullClient = createServiceClient(url);
    	_sourceType = SourceType.PULL_STREAM_SERVICE;
        if (logger.isDebugEnabled())
        	logger.debug("RETURN PullSourceWrapper()");
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
    		/* Convert the property document to a 
    		 * pull stream property document
    		 */
    		PullStreamPropertyDocumentType pullStreamPropDoc = 
    			(PullStreamPropertyDocumentType) propDoc;
    		StreamDescriptionType streamDesciption = 
    			pullStreamPropDoc.getStreamDescription();
    		if (streamDesciption != null) {
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
    				String msg = "Unknown schema definition format " + 
    					schemaFormatURI;
    				logger.warn(msg);
    				throw new SchemaMetadataException(msg);
    			}
    			extents  = extractSchema(schemaParser, ExtentType.PUSHED);
    		}
    		setStreamRates(extents, pullStreamPropDoc.getStreamRate());
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
    		logger.debug("RETURN getSchema() with " + extents);
    	return extents;
    }

    private void setStreamRates(List<ExtentMetadata> extents,
			List<StreamRateType> streamRates) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER setStreamRates() #extents=" + extents.size() +
					" #streamRates=" + streamRates.size());
		}
		for (ExtentMetadata extent : extents) {
			double rate = findAverageStreamRate(extent.getExtentName(), streamRates);
			extent.setRate(rate);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN setStreamRates()");
		}
	}

	/**
	 * Finds the declared average stream rate for the given extent name.
	 * If no rate has been declared, then a rate of 1.0 is assumed, i.e. 1Hz
	 * 
	 * @param extentName name of the extent 
	 * @param streamRates stream rates declared
	 * @return stream rate as a double
	 */
	private double findAverageStreamRate(String extentName,
			List<StreamRateType> streamRates) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER findAverageStreamRate() with " + extentName);
		}
		double rate = 1.0;
		for (StreamRateType streamRate : streamRates) {
			QName streamQName = streamRate.getStreamQName();
			logger.debug(streamQName);
			if (extentName.equalsIgnoreCase(streamQName.getLocalPart())) {
				rate = streamRate.getAverageFlowRate();
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN findAverageStreamRate() with " + rate);
		}
		return rate;
	}

	private PropertyDocumentType getPropertyDocument(String resourceName)
    throws SchemaMetadataException, InvalidResourceNameFault,
    DataResourceUnavailableFault, NotAuthorizedFault, ServiceBusyFault {
    	if (logger.isTraceEnabled()) {
    		logger.trace("ENTER getPropertyDocument() with " + 
    				resourceName);
    	}
    	GetDataResourcePropertyDocumentRequest request = 
    		new GetDataResourcePropertyDocumentRequest();
    	request.setDataResourceAbstractName(resourceName);
    	PropertyDocumentType propDoc = 
    		_pullClient.getPropertyDocument(request);
    	if (logger.isTraceEnabled()) {
    		logger.trace("RETURN getPropertyDocument() with " + 
    				propDoc.getDataResourceAbstractName());
    	}
    	return propDoc;
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
			createGetStreamItemRequest(resourceName, numItems, 
					timestamp);
			GenericQueryResponse response = 
				_pullClient.getStreamItems(request);
			List<Tuple> tuples;
			//XXX This is a bit of a hack!!! Be warned.
			/*
			 * If the client fails to connect to the service,
			 * it will return a null response. This happens 
			 * when the CCO is doing back up tasks over night.
			 * However, other services may behave differently.
			 * For now, just quitely ignoring the problem and 
			 * returning a zero tuple list.
			 */
			if (response == null) {
				tuples = new ArrayList<Tuple>();
			} else {
				tuples = processGenericQueryResponse(response);
			}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getData() #tuples=" + tuples.size());
		}
		return tuples;
	}

	public List<Tuple> getNewestData(String resourceName, 
			Integer numItems) 
	throws SNEEDataSourceException, TypeMappingException, 
	SchemaMetadataException, SNEEException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getNewestData() with " + resourceName +
					" #items=" + numItems);
		GetStreamNewestItemRequest request = 
			new GetStreamNewestItemRequest();
		request.setDataResourceAbstractName(resourceName);
		request.setDatasetFormatURI(DATASET_FORMAT);
		if (numItems != null) {
			request.setCount(numItems.toString());
		}
		request.setMaximumTuples(_maxTuples);
		GenericQueryResponse response = 
			_pullClient.getStreamNewestItem(request);;
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
		request.setDatasetFormatURI(DATASET_FORMAT);
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
		if (formatURI.trim().equalsIgnoreCase(DATASET_FORMAT)) {
			if (logger.isTraceEnabled()) {
				logger.trace("Processing dataset with format " +
						formatURI);
			}
			List<Object> datasets = 
				dataset.getDatasetData().getContent();
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
			logger.trace("RETURN processGenericResponse() with " +
					tuples );
		return tuples;
	}
    
}
