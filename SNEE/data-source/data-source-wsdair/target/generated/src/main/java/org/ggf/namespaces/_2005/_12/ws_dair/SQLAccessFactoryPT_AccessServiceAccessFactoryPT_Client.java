
package org.ggf.namespaces._2005._12.ws_dair;

/**
 * Please modify this class to meet your needs
 * This class is not complete
 */

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import javax.xml.namespace.QName;
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

public final class SQLAccessFactoryPT_AccessServiceAccessFactoryPT_Client {

    private static final QName SERVICE_NAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLAccessService");

    private SQLAccessFactoryPT_AccessServiceAccessFactoryPT_Client() {
    }

    public static void main(String args[]) throws Exception {
        URL wsdlURL = SQLAccessService.WSDL_LOCATION;
        if (args.length > 0) { 
            File wsdlFile = new File(args[0]);
            try {
                if (wsdlFile.exists()) {
                    wsdlURL = wsdlFile.toURI().toURL();
                } else {
                    wsdlURL = new URL(args[0]);
                }
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
      
        SQLAccessService ss = new SQLAccessService(wsdlURL, SERVICE_NAME);
        SQLAccessFactoryPT port = ss.getAccessServiceAccessFactoryPT();  
        
        {
        System.out.println("Invoking sqlExecuteFactory...");
        org.ggf.namespaces._2005._12.ws_dair.SQLExecuteFactoryRequest _sqlExecuteFactory_sqlExecuteFactoryRequest = new org.ggf.namespaces._2005._12.ws_dair.SQLExecuteFactoryRequest();
        _sqlExecuteFactory_sqlExecuteFactoryRequest.setDataResourceAbstractName("DataResourceAbstractName-317735554");
        _sqlExecuteFactory_sqlExecuteFactoryRequest.setPortTypeQName(new javax.xml.namespace.QName("http://PortTypeQName1762636738.com", "PortTypeQName887397023"));
        javax.xml.bind.JAXBElement<? extends org.ggf.namespaces._2005._12.ws_dai.ConfigurationDocumentType> _sqlExecuteFactory_sqlExecuteFactoryRequestConfigurationDocument = null;
        _sqlExecuteFactory_sqlExecuteFactoryRequest.setConfigurationDocument(_sqlExecuteFactory_sqlExecuteFactoryRequestConfigurationDocument);
        org.apache.cxf.ws.addressing.EndpointReferenceType _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetService = new org.apache.cxf.ws.addressing.EndpointReferenceType();
        org.apache.cxf.ws.addressing.AttributedURIType _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceAddress = new org.apache.cxf.ws.addressing.AttributedURIType();
        _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceAddress.setValue("Value-867631493");
        _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetService.setAddress(_sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceAddress);
        org.apache.cxf.ws.addressing.ReferenceParametersType _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceReferenceParameters = new org.apache.cxf.ws.addressing.ReferenceParametersType();
        java.util.List<java.lang.Object> _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceReferenceParametersAny = new java.util.ArrayList<java.lang.Object>();
        _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceReferenceParameters.getAny().addAll(_sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceReferenceParametersAny);
        _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetService.setReferenceParameters(_sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceReferenceParameters);
        org.apache.cxf.ws.addressing.MetadataType _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceMetadata = new org.apache.cxf.ws.addressing.MetadataType();
        java.util.List<java.lang.Object> _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceMetadataAny = new java.util.ArrayList<java.lang.Object>();
        _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceMetadata.getAny().addAll(_sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceMetadataAny);
        _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetService.setMetadata(_sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceMetadata);
        java.util.List<java.lang.Object> _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceAny = new java.util.ArrayList<java.lang.Object>();
        java.lang.Object _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceAnyVal1 = null;
        _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceAny.add(_sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceAnyVal1);
        _sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetService.getAny().addAll(_sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetServiceAny);
        _sqlExecuteFactory_sqlExecuteFactoryRequest.setPreferredTargetService(_sqlExecuteFactory_sqlExecuteFactoryRequestPreferredTargetService);
        org.ggf.namespaces._2005._12.ws_dair.SQLExpressionType _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpression = new org.ggf.namespaces._2005._12.ws_dair.SQLExpressionType();
        _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpression.setLanguage("Language1701868362");
        _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpression.setExpression("Expression-596524924");
        java.util.List<org.ggf.namespaces._2005._12.ws_dair.SQLParameterType> _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpressionSQLParameter = new java.util.ArrayList<org.ggf.namespaces._2005._12.ws_dair.SQLParameterType>();
        org.ggf.namespaces._2005._12.ws_dair.SQLParameterType _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpressionSQLParameterVal1 = new org.ggf.namespaces._2005._12.ws_dair.SQLParameterType();
        _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpressionSQLParameterVal1.setValue("Value1074295470");
        _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpressionSQLParameterVal1.setType("Type-171017235");
        _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpressionSQLParameterVal1.setMode("Mode1373377336");
        _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpressionSQLParameter.add(_sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpressionSQLParameterVal1);
        _sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpression.getSQLParameter().addAll(_sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpressionSQLParameter);
        _sqlExecuteFactory_sqlExecuteFactoryRequest.setSQLExpression(_sqlExecuteFactory_sqlExecuteFactoryRequestSQLExpression);
        try {
            org.ggf.namespaces._2005._12.ws_dai.DataResourceAddressListType _sqlExecuteFactory__return = port.sqlExecuteFactory(_sqlExecuteFactory_sqlExecuteFactoryRequest);
            System.out.println("sqlExecuteFactory.result=" + _sqlExecuteFactory__return);

        } catch (org.ggf.namespaces._2005._12.ws_dai.InvalidExpressionFault e) { 
            System.out.println("Expected exception: InvalidExpressionFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.NotAuthorizedFault e) { 
            System.out.println("Expected exception: NotAuthorizedFault has occurred.");
            System.out.println(e.toString());
        } catch (InvalidSQLExpressionParameterFault e) { 
            System.out.println("Expected exception: InvalidSQLExpressionParameterFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.InvalidLanguageFault e) { 
            System.out.println("Expected exception: InvalidLanguageFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.InvalidConfigurationDocumentFault e) { 
            System.out.println("Expected exception: InvalidConfigurationDocumentFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.DataResourceUnavailableFault e) { 
            System.out.println("Expected exception: DataResourceUnavailableFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.InvalidPortTypeQNameFault e) { 
            System.out.println("Expected exception: InvalidPortTypeQNameFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFault e) { 
            System.out.println("Expected exception: InvalidResourceNameFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.ServiceBusyFault e) { 
            System.out.println("Expected exception: ServiceBusyFault has occurred.");
            System.out.println(e.toString());
        }
            }

        System.exit(0);
    }

}
