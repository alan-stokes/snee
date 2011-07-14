package org.ggf.namespaces._2005._12.ws_dair;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.xml.bind.annotation.XmlSeeAlso;

/**
 * This class was generated by Apache CXF 2.2.8
 * Thu Jul 14 09:06:38 EEST 2011
 * Generated source version: 2.2.8
 * 
 */
 
@WebService(targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLAccessPT")
@XmlSeeAlso({com.sun.java.xml.ns.jdbc.ObjectFactory.class,org.apache.cxf.ws.addressing.ObjectFactory.class,ObjectFactory.class,org.ggf.namespaces._2005._12.ws_dai.ObjectFactory.class})
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface SQLAccessPT {

    @WebResult(name = "SQLExecuteResponse", targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", partName = "SQLExecuteResponse")
    @WebMethod(operationName = "SQLExecute", action = "http://www.ggf.org/namespaces/2005/12/WS-DAIR#SQLExecute")
    public SQLExecuteResponse sqlExecute(
        @WebParam(partName = "SQLExecuteRequest", name = "SQLExecuteRequest")
        SQLExecuteRequest sqlExecuteRequest
    ) throws org.ggf.namespaces._2005._12.ws_dai.InvalidExpressionFault, org.ggf.namespaces._2005._12.ws_dai.NotAuthorizedFault, InvalidSQLExpressionParameterFault, org.ggf.namespaces._2005._12.ws_dai.InvalidLanguageFault, org.ggf.namespaces._2005._12.ws_dai.DataResourceUnavailableFault, org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFault, org.ggf.namespaces._2005._12.ws_dai.ServiceBusyFault, org.ggf.namespaces._2005._12.ws_dai.InvalidDatasetFormatFault;

    @WebResult(name = "GetSQLPropertyDocumentResponse", targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", partName = "GetSQLPropertyDocumentResponse")
    @WebMethod(operationName = "GetSQLPropertyDocument", action = "http://www.ggf.org/namespaces/2005/12/WS-DAIR#GetSQLPropertyDocument")
    public SQLPropertyDocumentType getSQLPropertyDocument(
        @WebParam(partName = "GetSQLPropertyDocumentRequest", name = "GetSQLPropertyDocumentRequest")
        org.ggf.namespaces._2005._12.ws_dai.GetDataResourcePropertyDocumentRequest getSQLPropertyDocumentRequest
    ) throws org.ggf.namespaces._2005._12.ws_dai.NotAuthorizedFault, org.ggf.namespaces._2005._12.ws_dai.DataResourceUnavailableFault, org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFault, org.ggf.namespaces._2005._12.ws_dai.ServiceBusyFault;
}
