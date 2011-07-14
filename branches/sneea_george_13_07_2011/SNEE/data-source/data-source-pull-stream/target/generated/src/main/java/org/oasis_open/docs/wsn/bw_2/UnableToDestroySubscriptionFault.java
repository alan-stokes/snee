
package org.oasis_open.docs.wsn.bw_2;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Thu Jul 14 09:06:22 EEST 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "UnableToDestroySubscriptionFault", targetNamespace = "http://docs.oasis-open.org/wsn/b-2")
public class UnableToDestroySubscriptionFault extends Exception {
    public static final long serialVersionUID = 20110714090622L;
    
    private org.oasis_open.docs.wsn.b_2.UnableToDestroySubscriptionFaultType unableToDestroySubscriptionFault;

    public UnableToDestroySubscriptionFault() {
        super();
    }
    
    public UnableToDestroySubscriptionFault(String message) {
        super(message);
    }
    
    public UnableToDestroySubscriptionFault(String message, Throwable cause) {
        super(message, cause);
    }

    public UnableToDestroySubscriptionFault(String message, org.oasis_open.docs.wsn.b_2.UnableToDestroySubscriptionFaultType unableToDestroySubscriptionFault) {
        super(message);
        this.unableToDestroySubscriptionFault = unableToDestroySubscriptionFault;
    }

    public UnableToDestroySubscriptionFault(String message, org.oasis_open.docs.wsn.b_2.UnableToDestroySubscriptionFaultType unableToDestroySubscriptionFault, Throwable cause) {
        super(message, cause);
        this.unableToDestroySubscriptionFault = unableToDestroySubscriptionFault;
    }

    public org.oasis_open.docs.wsn.b_2.UnableToDestroySubscriptionFaultType getFaultInfo() {
        return this.unableToDestroySubscriptionFault;
    }
}
