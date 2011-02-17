
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
 * Fri Feb 04 16:28:11 GMT 2011
 * Generated source version: 2.2.8
 * 
 */

public final class SQLResponseFactoryPT_ResponseServiceResponseFactoryPT_Client {

    private static final QName SERVICE_NAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLResponseService");

    private SQLResponseFactoryPT_ResponseServiceResponseFactoryPT_Client() {
    }

    public static void main(String args[]) throws Exception {
        URL wsdlURL = SQLResponseService.WSDL_LOCATION;
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
      
        SQLResponseService ss = new SQLResponseService(wsdlURL, SERVICE_NAME);
        SQLResponseFactoryPT port = ss.getResponseServiceResponseFactoryPT();  
        
        {
        System.out.println("Invoking getSQLRowsetFactory...");
        org.ggf.namespaces._2005._12.ws_dair.GetSQLRowsetFactoryRequest _getSQLRowsetFactory_getSQLRowsetFactoryRequest = new org.ggf.namespaces._2005._12.ws_dair.GetSQLRowsetFactoryRequest();
        _getSQLRowsetFactory_getSQLRowsetFactoryRequest.setDataResourceAbstractName("DataResourceAbstractName-1453114885");
        _getSQLRowsetFactory_getSQLRowsetFactoryRequest.setPortTypeQName(new javax.xml.namespace.QName("http://PortTypeQName-1754425130.com", "PortTypeQName-1187620880"));
        javax.xml.bind.JAXBElement<? extends org.ggf.namespaces._2005._12.ws_dai.ConfigurationDocumentType> _getSQLRowsetFactory_getSQLRowsetFactoryRequestConfigurationDocument = null;
        _getSQLRowsetFactory_getSQLRowsetFactoryRequest.setConfigurationDocument(_getSQLRowsetFactory_getSQLRowsetFactoryRequestConfigurationDocument);
        org.apache.cxf.ws.addressing.EndpointReferenceType _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetService = new org.apache.cxf.ws.addressing.EndpointReferenceType();
        org.apache.cxf.ws.addressing.AttributedURIType _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceAddress = new org.apache.cxf.ws.addressing.AttributedURIType();
        _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceAddress.setValue("Value-591053027");
        _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetService.setAddress(_getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceAddress);
        org.apache.cxf.ws.addressing.ReferenceParametersType _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceReferenceParameters = new org.apache.cxf.ws.addressing.ReferenceParametersType();
        java.util.List<java.lang.Object> _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceReferenceParametersAny = new java.util.ArrayList<java.lang.Object>();
        _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceReferenceParameters.getAny().addAll(_getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceReferenceParametersAny);
        _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetService.setReferenceParameters(_getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceReferenceParameters);
        org.apache.cxf.ws.addressing.MetadataType _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceMetadata = new org.apache.cxf.ws.addressing.MetadataType();
        java.util.List<java.lang.Object> _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceMetadataAny = new java.util.ArrayList<java.lang.Object>();
        _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceMetadata.getAny().addAll(_getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceMetadataAny);
        _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetService.setMetadata(_getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceMetadata);
        java.util.List<java.lang.Object> _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceAny = new java.util.ArrayList<java.lang.Object>();
        java.lang.Object _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceAnyVal1 = null;
        _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceAny.add(_getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceAnyVal1);
        _getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetService.getAny().addAll(_getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetServiceAny);
        _getSQLRowsetFactory_getSQLRowsetFactoryRequest.setPreferredTargetService(_getSQLRowsetFactory_getSQLRowsetFactoryRequestPreferredTargetService);
        _getSQLRowsetFactory_getSQLRowsetFactoryRequest.setPosition(2153857780555897064l);
        _getSQLRowsetFactory_getSQLRowsetFactoryRequest.setCount(Long.valueOf(5041895398699467577l));
        try {
            org.ggf.namespaces._2005._12.ws_dai.DataResourceAddressListType _getSQLRowsetFactory__return = port.getSQLRowsetFactory(_getSQLRowsetFactory_getSQLRowsetFactoryRequest);
            System.out.println("getSQLRowsetFactory.result=" + _getSQLRowsetFactory__return);

        } catch (org.ggf.namespaces._2005._12.ws_dai.NotAuthorizedFault e) { 
            System.out.println("Expected exception: NotAuthorizedFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.DataResourceUnavailableFault e) { 
            System.out.println("Expected exception: DataResourceUnavailableFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFault e) { 
            System.out.println("Expected exception: InvalidResourceNameFault has occurred.");
            System.out.println(e.toString());
        } catch (InvalidPositionFault e) { 
            System.out.println("Expected exception: InvalidPositionFault has occurred.");
            System.out.println(e.toString());
        } catch (InvalidCountFault e) { 
            System.out.println("Expected exception: InvalidCountFault has occurred.");
            System.out.println(e.toString());
        } catch (org.ggf.namespaces._2005._12.ws_dai.ServiceBusyFault e) { 
            System.out.println("Expected exception: ServiceBusyFault has occurred.");
            System.out.println(e.toString());
        }
            }

        System.exit(0);
    }

}
