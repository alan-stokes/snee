package uk.ac.manchester.cs.snee.datasource.webservice;

import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import eu.semsorgrid4env.service.stream.pull.GetStreamItemRequest;
import eu.semsorgrid4env.service.stream.pull.GetStreamNewestItemRequest;
import eu.semsorgrid4env.service.stream.pull.InvalidCountFault;
import eu.semsorgrid4env.service.stream.pull.InvalidPositionFault;
import eu.semsorgrid4env.service.stream.pull.MaximumTuplesExceededFault;
import eu.semsorgrid4env.service.stream.pull.PullStreamInterface;
import eu.semsorgrid4env.service.stream.pull.PullStreamPropertyDocumentType;
import eu.semsorgrid4env.service.stream.pull.PullStreamService;
import eu.semsorgrid4env.service.wsdai.DataResourceUnavailableFault;
import eu.semsorgrid4env.service.wsdai.GenericQueryResponse;
import eu.semsorgrid4env.service.wsdai.GetDataResourcePropertyDocumentRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListResponse;
import eu.semsorgrid4env.service.wsdai.InvalidDatasetFormatFault;
import eu.semsorgrid4env.service.wsdai.InvalidResourceNameFault;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.PropertyDocumentType;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;

public class PullStreamServiceClient {

	private static final Logger logger = Logger.getLogger(PullStreamServiceClient.class.getName());

	private static final QName SERVICE_NAME = 
		new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", 
		"PullStreamService");
	private PullStreamInterface _pullSource;

	private String _serviceUrl;

	protected PullStreamServiceClient() {

	}

	public PullStreamServiceClient(String url) 
	throws MalformedURLException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER PullStreamServiceClient() with URL " +
					url);
		_serviceUrl = url;
		/*
		 * Using a method to construct the service so that it can
		 * be overridden as a mock object for testing
		 */
		_pullSource = createService(_serviceUrl);
		if (logger.isDebugEnabled())
			logger.debug("RETURN PullStreamServiceClient()");
	}

	private PullStreamInterface createService(String url) 
	throws MalformedURLException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createService() with URL " + url);
		PullStreamInterface pullSource;
		try {
			URL wsdlURL = new URL(url);
			PullStreamService ss = 
				new PullStreamService(wsdlURL, SERVICE_NAME);
			logger.trace("Service created");
			pullSource = ss.getPullStreamInterface();
			logger.trace("Port added to service");
		} catch (MalformedURLException e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		} 
		if (logger.isTraceEnabled())
			logger.trace("RETURN createService()");
		return pullSource;
	}

	public GetResourceListResponse getResourceList(
			GetResourceListRequest request) 
	throws NotAuthorizedFault, ServiceBusyFault 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER getResourceList()");
		GetResourceListResponse response;
		try {
			System.out.println(_pullSource.toString());
			response = _pullSource.getResourceList(request);
		} catch (NotAuthorizedFault e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		} catch (ServiceBusyFault e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getResourceList() with response " + 
					response);
		return response;
	}

	public PropertyDocumentType getPropertyDocument(
			GetDataResourcePropertyDocumentRequest request) 
	throws SchemaMetadataException, InvalidResourceNameFault, 
	DataResourceUnavailableFault, NotAuthorizedFault, 
	ServiceBusyFault {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getPropertyDocument() with " +
					request.getDataResourceAbstractName());
		PropertyDocumentType propDoc;
		try {
			propDoc =
				//    			_pullSource.getDataResourcePropertyDocument(request);
				(PullStreamPropertyDocumentType) _pullSource.getDataResourcePropertyDocument(request);
			//    	} catch (ClassCastException e) {
			//    		String msg = "Unable to construct a pull stream property document from the received property document.";
			//    		logger.warn(msg);
			//    		throw new SchemaMetadataException(msg);
		} catch (InvalidResourceNameFault e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		} catch (DataResourceUnavailableFault e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		} catch (NotAuthorizedFault e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		} catch (ServiceBusyFault e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getPropertyDocument() with property " +
					"document " + propDoc);
		return propDoc;
	}

	public GenericQueryResponse getStreamItems(
			GetStreamItemRequest request) 
	throws SNEEDataSourceException
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getStreamItemsFromService()" +
					"\n\tResource name: " + request.getDataResourceAbstractName() +
					"\n\tDataset Format: " + request.getDatasetFormatURI() +
					"\n\tPosition: " + request.getPosition() +
					"\n\tCount: " + request.getCount() +
					"\n\tMax Size: " + request.getMaximumTuples());
		}
		GenericQueryResponse response = null;
		try {
			response = _pullSource.getStreamItem(request);
		} catch (MaximumTuplesExceededFault e) {
			String msg = "Maximum number of tuples exceeded.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidPositionFault e) {
			String msg = "Invalid dataset position.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidCountFault e) {
			String msg = "Invalid count.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidResourceNameFault e) {
			String msg = "Resource name " + request.getDataResourceAbstractName() + 
			" unknown to service " + SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidDatasetFormatFault e) {
			String msg = "Invalid dataset format requested.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (DataResourceUnavailableFault e) {
			String msg = "Resource " + request.getDataResourceAbstractName() + 
			" currently unavailable on " + SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (NotAuthorizedFault e) {
			String msg = "Not authorised to use service " + SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (ServiceBusyFault e) {
			String msg = "Service " + SERVICE_NAME + " is busy.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (javax.xml.ws.WebServiceException e) {
			/*
			 * Catching runtime exception when remote service is not
			 * available. For now, just log and carry on.
			 */
			logger.warn("Unable to connect to service " +
					_serviceUrl + " with resource "+ 
					request.getDataResourceAbstractName() +
					". Carry on.");
		} finally {
			if (response == null) {
				logger.warn("Unexpected problem connecting to " +
						_serviceUrl + " when issuing GetStreamItems " +
						"call with parameters:" +
						"\n\tResource name: " + request.getDataResourceAbstractName() +
						"\n\tDataset Format: " + request.getDatasetFormatURI() +
						"\n\tPosition: " + request.getPosition() +
						"\n\tCount: " + request.getCount() +
						"\n\tMax Size: " + request.getMaximumTuples());
			}
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN getStreamItemsFromService() with " +
					"response: " + response);
		return response;
	}

	public GenericQueryResponse getStreamNewestItem(
			GetStreamNewestItemRequest request) 
	throws SNEEDataSourceException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getStreamNewestItem()" +
					"\n\tResource name: " + request.getDataResourceAbstractName() +
					"\n\tDataset Format: " + request.getDatasetFormatURI() +
					"\n\tCount: " + request.getCount() +
					"\n\tMax Size: " + request.getMaximumTuples());
		}
		GenericQueryResponse response = null;
		try {
			response = _pullSource.getStreamNewestItem(request);
		} catch (MaximumTuplesExceededFault e) {
			String msg = "Maximum number of tuples exceeded.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidResourceNameFault e) {
			String msg = "Resource name " + request.getDataResourceAbstractName() + 
			" unknown to service " + SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (DataResourceUnavailableFault e) {
			String msg = "Resource " + request.getDataResourceAbstractName() + 
			" currently unavailable on " + SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidCountFault e) {
			String msg = "Invalid count.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (ServiceBusyFault e) {
			String msg = "Service " + SERVICE_NAME + " is busy.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (NotAuthorizedFault e) {
			String msg = "Not authorised to use service " + SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidDatasetFormatFault e) {
			String msg = "Invalid dataset format requested.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (javax.xml.ws.WebServiceException e) {
			/*
			 * Catching runtime exception when remote service is not
			 * available. For now, just log and carry on.
			 */
			logger.warn("Unable to connect to service " +
					_serviceUrl + " with resource "+ 
					request.getDataResourceAbstractName() +
					". Carry on.");
		} finally {
			if (response == null) {
				logger.warn("Unexpected problem connecting to " +
						_serviceUrl + " when issuing GetStreamItems " +
						"call with parameters:" +
						"\n\tResource name: " + request.getDataResourceAbstractName() +
						"\n\tDataset Format: " + request.getDatasetFormatURI() +
						"\n\tCount: " + request.getCount() +
						"\n\tMax Size: " + request.getMaximumTuples());
			}
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getStreamNewestItem() with response " +
					response);
		return response;
	}

}
