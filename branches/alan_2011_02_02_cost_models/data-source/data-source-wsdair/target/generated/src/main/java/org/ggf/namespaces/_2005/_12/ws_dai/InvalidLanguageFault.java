
package org.ggf.namespaces._2005._12.ws_dai;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Fri Feb 04 16:28:16 GMT 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "InvalidLanguageFault", targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI")
public class InvalidLanguageFault extends Exception {
    public static final long serialVersionUID = 20110204162816L;
    
    private org.ggf.namespaces._2005._12.ws_dai.InvalidLanguageFaultType invalidLanguageFault;

    public InvalidLanguageFault() {
        super();
    }
    
    public InvalidLanguageFault(String message) {
        super(message);
    }
    
    public InvalidLanguageFault(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidLanguageFault(String message, org.ggf.namespaces._2005._12.ws_dai.InvalidLanguageFaultType invalidLanguageFault) {
        super(message);
        this.invalidLanguageFault = invalidLanguageFault;
    }

    public InvalidLanguageFault(String message, org.ggf.namespaces._2005._12.ws_dai.InvalidLanguageFaultType invalidLanguageFault, Throwable cause) {
        super(message, cause);
        this.invalidLanguageFault = invalidLanguageFault;
    }

    public org.ggf.namespaces._2005._12.ws_dai.InvalidLanguageFaultType getFaultInfo() {
        return this.invalidLanguageFault;
    }
}
