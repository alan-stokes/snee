
package org.ggf.namespaces._2005._12.ws_dai;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Fri Feb 04 16:28:05 GMT 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "InvalidPortTypeQNameFault", targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI")
public class InvalidPortTypeQNameFault extends Exception {
    public static final long serialVersionUID = 20110204162805L;
    
    private org.ggf.namespaces._2005._12.ws_dai.InvalidPortTypeQNameFaultType invalidPortTypeQNameFault;

    public InvalidPortTypeQNameFault() {
        super();
    }
    
    public InvalidPortTypeQNameFault(String message) {
        super(message);
    }
    
    public InvalidPortTypeQNameFault(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidPortTypeQNameFault(String message, org.ggf.namespaces._2005._12.ws_dai.InvalidPortTypeQNameFaultType invalidPortTypeQNameFault) {
        super(message);
        this.invalidPortTypeQNameFault = invalidPortTypeQNameFault;
    }

    public InvalidPortTypeQNameFault(String message, org.ggf.namespaces._2005._12.ws_dai.InvalidPortTypeQNameFaultType invalidPortTypeQNameFault, Throwable cause) {
        super(message, cause);
        this.invalidPortTypeQNameFault = invalidPortTypeQNameFault;
    }

    public org.ggf.namespaces._2005._12.ws_dai.InvalidPortTypeQNameFaultType getFaultInfo() {
        return this.invalidPortTypeQNameFault;
    }
}
