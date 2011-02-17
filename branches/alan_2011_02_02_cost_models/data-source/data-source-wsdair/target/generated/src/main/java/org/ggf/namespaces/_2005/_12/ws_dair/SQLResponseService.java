
/*
 * 
 */

package org.ggf.namespaces._2005._12.ws_dair;

import java.net.MalformedURLException;
import java.net.URL;
import javax.xml.namespace.QName;
import javax.xml.ws.WebEndpoint;
import javax.xml.ws.WebServiceClient;
import javax.xml.ws.WebServiceFeature;
import javax.xml.ws.Service;
import org.ggf.namespaces._2005._12.ws_dai.CoreDataAccessPT;
import org.ggf.namespaces._2005._12.ws_dai.CoreResourceListPT;

/**
 * This class was generated by Apache CXF 2.2.8
 * Fri Feb 04 16:28:11 GMT 2011
 * Generated source version: 2.2.8
 * 
 */


@WebServiceClient(name = "SQLResponseService", 
                  wsdlLocation = "file:/home/S06/stokesa6/PHD/Dropbox/SNEE_Cost_Models/SNEE/data-source/data-source-wsdair/target/wsdl/wsdair/sqlresponse_service_service.wsdl",
                  targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR") 
public class SQLResponseService extends Service {

    public final static URL WSDL_LOCATION;
    public final static QName SERVICE = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLResponseService");
    public final static QName ResponseServiceResponseFactoryPT = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "ResponseServiceResponseFactoryPT");
    public final static QName ResponseServiceCoreDataAccessPT = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "ResponseServiceCoreDataAccessPT");
    public final static QName ResponseServiceResponsePT = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "ResponseServiceResponsePT");
    public final static QName ResponseServiceCoreResourceListPT = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "ResponseServiceCoreResourceListPT");
    static {
        URL url = null;
        try {
            url = new URL("file:/home/S06/stokesa6/PHD/Dropbox/SNEE_Cost_Models/SNEE/data-source/data-source-wsdair/target/wsdl/wsdair/sqlresponse_service_service.wsdl");
        } catch (MalformedURLException e) {
            System.err.println("Can not initialize the default wsdl from file:/home/S06/stokesa6/PHD/Dropbox/SNEE_Cost_Models/SNEE/data-source/data-source-wsdair/target/wsdl/wsdair/sqlresponse_service_service.wsdl");
            // e.printStackTrace();
        }
        WSDL_LOCATION = url;
    }

    public SQLResponseService(URL wsdlLocation) {
        super(wsdlLocation, SERVICE);
    }

    public SQLResponseService(URL wsdlLocation, QName serviceName) {
        super(wsdlLocation, serviceName);
    }

    public SQLResponseService() {
        super(WSDL_LOCATION, SERVICE);
    }

    /**
     * 
     * @return
     *     returns SQLResponseFactoryPT
     */
    @WebEndpoint(name = "ResponseServiceResponseFactoryPT")
    public SQLResponseFactoryPT getResponseServiceResponseFactoryPT() {
        return super.getPort(ResponseServiceResponseFactoryPT, SQLResponseFactoryPT.class);
    }

    /**
     * 
     * @param features
     *     A list of {@link javax.xml.ws.WebServiceFeature} to configure on the proxy.  Supported features not in the <code>features</code> parameter will have their default values.
     * @return
     *     returns SQLResponseFactoryPT
     */
    @WebEndpoint(name = "ResponseServiceResponseFactoryPT")
    public SQLResponseFactoryPT getResponseServiceResponseFactoryPT(WebServiceFeature... features) {
        return super.getPort(ResponseServiceResponseFactoryPT, SQLResponseFactoryPT.class, features);
    }
    /**
     * 
     * @return
     *     returns CoreDataAccessPT
     */
    @WebEndpoint(name = "ResponseServiceCoreDataAccessPT")
    public CoreDataAccessPT getResponseServiceCoreDataAccessPT() {
        return super.getPort(ResponseServiceCoreDataAccessPT, CoreDataAccessPT.class);
    }

    /**
     * 
     * @param features
     *     A list of {@link javax.xml.ws.WebServiceFeature} to configure on the proxy.  Supported features not in the <code>features</code> parameter will have their default values.
     * @return
     *     returns CoreDataAccessPT
     */
    @WebEndpoint(name = "ResponseServiceCoreDataAccessPT")
    public CoreDataAccessPT getResponseServiceCoreDataAccessPT(WebServiceFeature... features) {
        return super.getPort(ResponseServiceCoreDataAccessPT, CoreDataAccessPT.class, features);
    }
    /**
     * 
     * @return
     *     returns SQLResponsePT
     */
    @WebEndpoint(name = "ResponseServiceResponsePT")
    public SQLResponsePT getResponseServiceResponsePT() {
        return super.getPort(ResponseServiceResponsePT, SQLResponsePT.class);
    }

    /**
     * 
     * @param features
     *     A list of {@link javax.xml.ws.WebServiceFeature} to configure on the proxy.  Supported features not in the <code>features</code> parameter will have their default values.
     * @return
     *     returns SQLResponsePT
     */
    @WebEndpoint(name = "ResponseServiceResponsePT")
    public SQLResponsePT getResponseServiceResponsePT(WebServiceFeature... features) {
        return super.getPort(ResponseServiceResponsePT, SQLResponsePT.class, features);
    }
    /**
     * 
     * @return
     *     returns CoreResourceListPT
     */
    @WebEndpoint(name = "ResponseServiceCoreResourceListPT")
    public CoreResourceListPT getResponseServiceCoreResourceListPT() {
        return super.getPort(ResponseServiceCoreResourceListPT, CoreResourceListPT.class);
    }

    /**
     * 
     * @param features
     *     A list of {@link javax.xml.ws.WebServiceFeature} to configure on the proxy.  Supported features not in the <code>features</code> parameter will have their default values.
     * @return
     *     returns CoreResourceListPT
     */
    @WebEndpoint(name = "ResponseServiceCoreResourceListPT")
    public CoreResourceListPT getResponseServiceCoreResourceListPT(WebServiceFeature... features) {
        return super.getPort(ResponseServiceCoreResourceListPT, CoreResourceListPT.class, features);
    }

}
