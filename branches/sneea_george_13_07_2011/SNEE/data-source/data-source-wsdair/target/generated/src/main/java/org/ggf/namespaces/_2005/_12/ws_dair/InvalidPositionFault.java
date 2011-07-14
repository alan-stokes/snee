
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Thu Jul 14 09:06:52 EEST 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "InvalidPositionFault", targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR")
public class InvalidPositionFault extends Exception {
    public static final long serialVersionUID = 20110714090652L;
    
    private org.ggf.namespaces._2005._12.ws_dair.InvalidPositionFaultType invalidPositionFault;

    public InvalidPositionFault() {
        super();
    }
    
    public InvalidPositionFault(String message) {
        super(message);
    }
    
    public InvalidPositionFault(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidPositionFault(String message, org.ggf.namespaces._2005._12.ws_dair.InvalidPositionFaultType invalidPositionFault) {
        super(message);
        this.invalidPositionFault = invalidPositionFault;
    }

    public InvalidPositionFault(String message, org.ggf.namespaces._2005._12.ws_dair.InvalidPositionFaultType invalidPositionFault, Throwable cause) {
        super(message, cause);
        this.invalidPositionFault = invalidPositionFault;
    }

    public org.ggf.namespaces._2005._12.ws_dair.InvalidPositionFaultType getFaultInfo() {
        return this.invalidPositionFault;
    }
}
