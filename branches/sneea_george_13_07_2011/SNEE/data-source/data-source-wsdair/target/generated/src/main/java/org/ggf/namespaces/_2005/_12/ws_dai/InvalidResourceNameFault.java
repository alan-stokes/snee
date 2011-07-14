
package org.ggf.namespaces._2005._12.ws_dai;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Thu Jul 14 09:06:52 EEST 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "InvalidResourceNameFault", targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI")
public class InvalidResourceNameFault extends Exception {
    public static final long serialVersionUID = 20110714090652L;
    
    private org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFaultType invalidResourceNameFault;

    public InvalidResourceNameFault() {
        super();
    }
    
    public InvalidResourceNameFault(String message) {
        super(message);
    }
    
    public InvalidResourceNameFault(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidResourceNameFault(String message, org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFaultType invalidResourceNameFault) {
        super(message);
        this.invalidResourceNameFault = invalidResourceNameFault;
    }

    public InvalidResourceNameFault(String message, org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFaultType invalidResourceNameFault, Throwable cause) {
        super(message, cause);
        this.invalidResourceNameFault = invalidResourceNameFault;
    }

    public org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFaultType getFaultInfo() {
        return this.invalidResourceNameFault;
    }
}
