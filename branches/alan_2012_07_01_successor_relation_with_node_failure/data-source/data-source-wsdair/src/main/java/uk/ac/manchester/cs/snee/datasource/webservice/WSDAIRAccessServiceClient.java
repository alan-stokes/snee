package uk.ac.manchester.cs.snee.datasource.webservice;

import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPFaultException;

import org.apache.log4j.Logger;
import org.ggf.namespaces._2005._12.ws_dai.CoreDataAccessPT;
import org.ggf.namespaces._2005._12.ws_dai.CoreResourceListPT;
import org.ggf.namespaces._2005._12.ws_dai.DataResourceUnavailableFault;
import org.ggf.namespaces._2005._12.ws_dai.GetDataResourcePropertyDocumentRequest;
import org.ggf.namespaces._2005._12.ws_dai.GetResourceListRequest;
import org.ggf.namespaces._2005._12.ws_dai.GetResourceListResponse;
import org.ggf.namespaces._2005._12.ws_dai.InvalidDatasetFormatFault;
import org.ggf.namespaces._2005._12.ws_dai.InvalidExpressionFault;
import org.ggf.namespaces._2005._12.ws_dai.InvalidLanguageFault;
import org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFault;
import org.ggf.namespaces._2005._12.ws_dai.NotAuthorizedFault;
import org.ggf.namespaces._2005._12.ws_dai.PropertyDocumentType;
import org.ggf.namespaces._2005._12.ws_dai.ServiceBusyFault;
import org.ggf.namespaces._2005._12.ws_dair.InvalidSQLExpressionParameterFault;
import org.ggf.namespaces._2005._12.ws_dair.SQLAccessPT;
import org.ggf.namespaces._2005._12.ws_dair.SQLExecuteRequest;
import org.ggf.namespaces._2005._12.ws_dair.SQLExecuteResponse;
import org.ggf.namespaces._2005._12.ws_dair.SQLPropertyDocumentType;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

public class WSDAIRAccessServiceClient {

	private static final Logger logger = Logger.getLogger(WSDAIRAccessServiceClient.class.getName());

	/**
	 * EPR for all of the services offered, i.e. not the one including 
	 * the service name and wsdl.
	 * This is consistent with the SERVICE_BASE discussed in the
	 * WS-DAIR documentation.
	 */
	private String SERVICE_BASE;

	private static final String WSDAIR_NAMESPACE =
		"http://www.ggf.org/namespaces/2005/12/WS-DAIR";
	
	static final QName SERVICE_NAME = 
		new QName(WSDAIR_NAMESPACE, "SQLAccessService");

	/**
	 * Core Resource List port name.
	 * Provide the following operations:
	 *  * getResourceList
	 * 	* resolve 
	 */
	static final QName RESOURCE_LIST_PORT_NAME = 
		new QName(WSDAIR_NAMESPACE, "AccessServiceCoreResourceListPT");	

	/**
	 * Core Resource List port name.
	 * Provide the following operations:
	 *  * getDataResourcePropertyDocument
	 * 	* destroyDataResource
	 * 	* genericQuery
	 */
	static final QName CORE_ACCESS_PORT_NAME = 
		new QName(WSDAIR_NAMESPACE, "AccessServiceCoreDataAccessPT");	

	/**
	 * Core Resource List port name.
	 * Provide the following operations:
	 *  * getSQLPropertyDocument
	 * 	* SQLExecute
	 */
	static final QName SQL_ACCESS_PORT_NAME = 
		new QName(WSDAIR_NAMESPACE, "AccessServiceAccessPT");

	/**
	 * Client providing access to the following operations:
	 *  * getResourceList
	 * 	* resolve 
	 */
	private CoreResourceListPT _wsdairCorePT;

	/**
	 * Client providing access to the following operations:
	 *  * getDataResourcePropertyDocument
	 * 	* destroyDataResource
	 * 	* genericQuery
	 */
	private CoreDataAccessPT _wsdairCoreAccessPT;

	/**
	 * Client providing access to the following operations:
	 *  * getSQLPropertyDocument
	 * 	* SQLExecute
	 */
	private SQLAccessPT _wsdairAccessPT;

	protected WSDAIRAccessServiceClient() {

	}

	public WSDAIRAccessServiceClient(String url) 
	throws MalformedURLException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER WSDAIRAccessServiceClient() with URL " +
					url);
		SERVICE_BASE = url;
		/*
		 * Using methods to construct the service clients so that they can
		 * be overridden as a mock objects for testing
		 */
		_wsdairCorePT = createCorePT();
		_wsdairCoreAccessPT = createCoreAccessPT();
		_wsdairAccessPT = createSQLAccessPT();
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN WSDAIRAccessServiceClient()");
	}
	
	private CoreResourceListPT createCorePT() 
	throws MalformedURLException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createCorePT()");
		}
		URL coreResourceListWsdlURL = 
			new URL(SERVICE_BASE +
			"AccessServiceCoreResourceListPT?wsdl");
		logger.trace("WSDL URL: " + coreResourceListWsdlURL);
		Service service = 
			Service.create(coreResourceListWsdlURL, SERVICE_NAME);
		logger.trace("Service created: " + service.toString() + "\t" + 
				service.getServiceName());
		CoreResourceListPT coreResourceListClient = 
			service.getPort(RESOURCE_LIST_PORT_NAME, 
					CoreResourceListPT.class);
		if (logger.isTraceEnabled()) {
			logger.trace("CoreResourceListClient created: " + coreResourceListClient);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createCorePT()");
		}
		return coreResourceListClient;
	}
	
	private CoreDataAccessPT createCoreAccessPT() 
	throws MalformedURLException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createAccessCorePT()");
		}
		URL coreAccessWsdlURL = 
			new URL(SERVICE_BASE +
			"AccessServiceCoreDataAccessPT?wsdl");
		logger.trace("WSDL URL: " + coreAccessWsdlURL);
		Service service = 
			Service.create(coreAccessWsdlURL, SERVICE_NAME);
		logger.trace("Service created: " + service.toString() + "\t" + 
				service.getServiceName());
		CoreDataAccessPT coreDataAccessClient = 
			service.getPort(CORE_ACCESS_PORT_NAME, 
					CoreDataAccessPT.class);
		if (logger.isTraceEnabled()) {
			logger.trace("CoreDataAccessPTClient created: " + coreDataAccessClient);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createAccessCorePT()");
		}
		return coreDataAccessClient;
	}
	
	private SQLAccessPT createSQLAccessPT() 
	throws MalformedURLException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createSQLAccessPT()");
		}
		URL sqlAccessWsdlURL = 
			new URL(SERVICE_BASE +
			"AccessServiceAccessPT?wsdl");
		logger.trace("WSDL URL: " + sqlAccessWsdlURL);
		Service service = Service.create(sqlAccessWsdlURL, SERVICE_NAME);
		logger.trace("Service created: " + service.toString() + "\t" + 
				service.getServiceName());
		SQLAccessPT sqlAccessClient = service.getPort(SQL_ACCESS_PORT_NAME, 
				SQLAccessPT.class);
		if (logger.isTraceEnabled()) {
			logger.trace("SQLAccessClient created: " + sqlAccessClient);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createSQLAccessPT()");
		}
		return sqlAccessClient;
	}
	
	public GetResourceListResponse getResourceList(
			GetResourceListRequest request) 
	throws NotAuthorizedFault, ServiceBusyFault, SNEEDataSourceException 
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResourceList() with " + request);
		}
		GetResourceListResponse response;
		try {
			logger.debug("Accessing: " + _wsdairCorePT.toString());
			response = _wsdairCorePT.getResourceList(request);
		} catch (NotAuthorizedFault e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		} catch (ServiceBusyFault e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNEEDataSourceException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getResourceList() with response " + 
					response);
		return response;
	}

	public SQLPropertyDocumentType getSQLPropertyDocument(
			GetDataResourcePropertyDocumentRequest request) 
	throws SchemaMetadataException, InvalidResourceNameFault, 
	DataResourceUnavailableFault, NotAuthorizedFault, 
	ServiceBusyFault, SNEEDataSourceException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getSQLPropertyDocument() with " +
					request.getDataResourceAbstractName());
		SQLPropertyDocumentType propDoc;
		try {
			propDoc = _wsdairAccessPT.getSQLPropertyDocument(request);
		} catch (SOAPFaultException e) {
			logger.warn("Remote service error. " + e.getLocalizedMessage());
			throw new SNEEDataSourceException("Wrong service type");
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
				_wsdairCoreAccessPT.getDataResourcePropertyDocument(request);
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
	
	public SQLExecuteResponse sqlExecute(SQLExecuteRequest request) 
	throws SNEEDataSourceException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER sqlExecute()" +
					"\n\tResource name: " + request.getDataResourceAbstractName() +
					"\n\tDataset Format: " + request.getDatasetFormatURI() +
					"\n\tExpression: " + request.getSQLExpression());
		}
		SQLExecuteResponse response;
		try {
			response = _wsdairAccessPT.sqlExecute(request);
		} catch (InvalidExpressionFault e) {
			String message = "Invalid query expression.";
			logger.warn(message, e);
			throw new SNEEDataSourceException(message, e);
		} catch (NotAuthorizedFault e) {
			String msg = "Not authorised to use service " + 
				SERVICE_BASE + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidSQLExpressionParameterFault e) {
			String message = "Invalid query parameter.";
			logger.warn(message, e);
			throw new SNEEDataSourceException(message, e);
		} catch (InvalidLanguageFault e) {
			String message = "Service does not support language.";
			logger.warn(message, e);
			throw new SNEEDataSourceException(message, e);
		} catch (DataResourceUnavailableFault e) {
			String msg = "Resource " + 
				request.getDataResourceAbstractName() + 
				" currently unavailable on " + SERVICE_BASE + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidResourceNameFault e) {
			String msg = "Resource name " + 
				request.getDataResourceAbstractName() + 
				" unknown to service " + SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (ServiceBusyFault e) {
			String msg = "Service " + SERVICE_NAME + " is busy.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidDatasetFormatFault e) {
			String msg = "Invalid dataset format requested.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN sqlExecute() with " +
					"response: " + response);
		return response;
	}
	
}