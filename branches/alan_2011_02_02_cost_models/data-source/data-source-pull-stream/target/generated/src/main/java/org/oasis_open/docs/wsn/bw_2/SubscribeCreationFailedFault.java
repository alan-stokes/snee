
package org.oasis_open.docs.wsn.bw_2;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Fri Feb 04 16:27:51 GMT 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "SubscribeCreationFailedFault", targetNamespace = "http://docs.oasis-open.org/wsn/b-2")
public class SubscribeCreationFailedFault extends Exception {
    public static final long serialVersionUID = 20110204162751L;
    
    private org.oasis_open.docs.wsn.b_2.SubscribeCreationFailedFaultType subscribeCreationFailedFault;

    public SubscribeCreationFailedFault() {
        super();
    }
    
    public SubscribeCreationFailedFault(String message) {
        super(message);
    }
    
    public SubscribeCreationFailedFault(String message, Throwable cause) {
        super(message, cause);
    }

    public SubscribeCreationFailedFault(String message, org.oasis_open.docs.wsn.b_2.SubscribeCreationFailedFaultType subscribeCreationFailedFault) {
        super(message);
        this.subscribeCreationFailedFault = subscribeCreationFailedFault;
    }

    public SubscribeCreationFailedFault(String message, org.oasis_open.docs.wsn.b_2.SubscribeCreationFailedFaultType subscribeCreationFailedFault, Throwable cause) {
        super(message, cause);
        this.subscribeCreationFailedFault = subscribeCreationFailedFault;
    }

    public org.oasis_open.docs.wsn.b_2.SubscribeCreationFailedFaultType getFaultInfo() {
        return this.subscribeCreationFailedFault;
    }
}
