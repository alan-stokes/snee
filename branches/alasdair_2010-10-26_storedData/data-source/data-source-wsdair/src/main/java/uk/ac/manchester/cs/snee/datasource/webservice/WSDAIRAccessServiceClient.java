package uk.ac.manchester.cs.snee.datasource.webservice;

import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.log4j.Logger;
import org.ggf.namespaces._2005._12.ws_dair.InvalidSQLExpressionParameterFault;
import org.ggf.namespaces._2005._12.ws_dair.SQLAccessPT;
import org.ggf.namespaces._2005._12.ws_dair.SQLAccessService;
import org.ggf.namespaces._2005._12.ws_dair.SQLExecuteRequest;
import org.ggf.namespaces._2005._12.ws_dair.SQLExecuteResponse;
import org.ggf.namespaces._2005._12.ws_dair.SQLPropertyDocumentType;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import eu.semsorgrid4env.service.wsdai.CoreDataAccessPT;
import eu.semsorgrid4env.service.wsdai.CoreResourceListPT;
import eu.semsorgrid4env.service.wsdai.DataResourceUnavailableFault;
import eu.semsorgrid4env.service.wsdai.GetDataResourcePropertyDocumentRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListResponse;
import eu.semsorgrid4env.service.wsdai.InvalidDatasetFormatFault;
import eu.semsorgrid4env.service.wsdai.InvalidExpressionFault;
import eu.semsorgrid4env.service.wsdai.InvalidLanguageFault;
import eu.semsorgrid4env.service.wsdai.InvalidResourceNameFault;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.PropertyDocumentType;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;

public class WSDAIRAccessServiceClient {
	private static final QName SERVICE_NAME = 
		new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLAccessService");

	private static Logger logger = 
		Logger.getLogger(WSDAIRAccessServiceClient.class.getName());

//	private static final String WSDAI_NAMESPACE = 
//		"http://www.ggf.org/namespaces/2005/12/WS-DAI";

	private static final String WSDAIR_NAMESPACE =
		"http://www.ggf.org/namespaces/2005/12/WS-DAIR";

//	private static final QName CORE_DATA_ACCESS_SERVICE_NAME = 
//		new QName(WSDAI_NAMESPACE, 
//		"CoreDataAccessPT");

	private static final QName SQL_ACCESS_SERVICE_NAME = 
		new QName(WSDAIR_NAMESPACE, 
		"SQLAccessService");

	private String _serviceUrl;

	private SQLAccessPT _wsdairAccessPT;

	private CoreResourceListPT _wsdairCorePT;

	private CoreDataAccessPT _wsdairCoreAccessPT;

	protected WSDAIRAccessServiceClient() {

	}

	public WSDAIRAccessServiceClient(String url) 
	throws MalformedURLException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER WSDAIRAccessServiceClient() with URL " +
					url);
		_serviceUrl = url;
		/*
		 * Using a method to construct the service so that it can
		 * be overridden as a mock object for testing
		 * 
		 * URL is expected to be the EPR for all of the services, 
		 * i.e. not the one including the service name and wsdl,
		 * the rest is added programmatically.
		 */
		SQLAccessService accessService = 
			createSQLAccessService(_serviceUrl);
		_wsdairCorePT = createCorePT(accessService);
		_wsdairCoreAccessPT = createCoreAccessPT(accessService);
		_wsdairAccessPT = createSQLAccessPT(accessService);
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN WSDAIRAccessServiceClient()");
	}

	private SQLAccessService createSQLAccessService(String url) 
	throws MalformedURLException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createService() with URL " + url);
		SQLAccessService ss;
		try {
			URL wsdlURL = new URL(url + "AccessServiceAccessPT?wsdl");
			ss = new SQLAccessService(wsdlURL, SQL_ACCESS_SERVICE_NAME);
//			System.out.println(ss.toString());
			logger.trace("Service created");
		} catch (MalformedURLException e) {
			logger.warn(e.getLocalizedMessage());
			throw e;
		} 
		if (logger.isTraceEnabled())
			logger.trace("RETURN createService()");
		return ss;
	}
	
	private CoreResourceListPT createCorePT(SQLAccessService accessService) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createCorePT()");
		}
		CoreResourceListPT coreResourceSource = 
			accessService.getAccessServiceCoreResourceListPT();
		if (logger.isTraceEnabled()) {
			logger.trace("Port added to service.\n\t" + coreResourceSource);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createCorePT()");
		}
		return coreResourceSource;
	}
	
	private CoreDataAccessPT createCoreAccessPT(SQLAccessService accessService) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createAccessCorePT()");
		}
		CoreDataAccessPT coreAccessSource = 
			accessService.getAccessServiceCoreDataAccessPT();
		if (logger.isTraceEnabled()) {
			logger.trace("Port added to service.\n\t" + coreAccessSource);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createAccessCorePT()");
		}
		return coreAccessSource;
	}
	
	private SQLAccessPT createSQLAccessPT(SQLAccessService accessService) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createSQLAccessPT()");
		}
		SQLAccessPT sqlAccessSource = 
			accessService.getAccessServiceAccessPT();
		if (logger.isTraceEnabled()) {
			logger.trace("Port added to service.\n\t" + sqlAccessSource);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createSQLAccessPT()");
		}
		return sqlAccessSource;
	}

//    System.out.println("Invoking getResourceList...");
//    eu.semsorgrid4env.service.wsdai.GetResourceListRequest 
//	_getResourceList_getResourceListRequest = 
//	new eu.semsorgrid4env.service.wsdai.GetResourceListRequest();
//    try {
//        eu.semsorgrid4env.service.wsdai.GetResourceListResponse 
//	_getResourceList__return = 
//	port.getResourceList(_getResourceList_getResourceListRequest);
//        System.out.println("getResourceList.result=" + _getResourceList__return);
//
//    } catch (ServiceBusyFault e) { 
//        System.out.println("Expected exception: ServiceBusyFault has occurred.");
//        System.out.println(e.toString());
//    } catch (NotAuthorizedFault e) { 
//        System.out.println("Expected exception: NotAuthorizedFault has occurred.");
//        System.out.println(e.toString());
//    }

	
	public GetResourceListResponse getResourceList(
			GetResourceListRequest request) 
	throws NotAuthorizedFault, ServiceBusyFault, SNEEDataSourceException 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER getResourceList()");
		GetResourceListResponse response;
		try {
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
			propDoc = (SQLPropertyDocumentType)
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
				SQL_ACCESS_SERVICE_NAME + ".";
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
				" currently unavailable on " + SQL_ACCESS_SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidResourceNameFault e) {
			String msg = "Resource name " + 
				request.getDataResourceAbstractName() + 
				" unknown to service " + SQL_ACCESS_SERVICE_NAME + ".";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (ServiceBusyFault e) {
			String msg = "Service " + SQL_ACCESS_SERVICE_NAME + " is busy.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		} catch (InvalidDatasetFormatFault e) {
			String msg = "Invalid dataset format requested.";
			logger.warn(msg, e);
			throw new SNEEDataSourceException(msg, e);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN sqlExecute() with " +
					"response: " + response);
		return response;
	}
	
}