
package org.oasis_open.docs.wsn.bw_2;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Thu Jul 14 09:06:21 EEST 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "UnsupportedPolicyRequestFault", targetNamespace = "http://docs.oasis-open.org/wsn/b-2")
public class UnsupportedPolicyRequestFault extends Exception {
    public static final long serialVersionUID = 20110714090621L;
    
    private org.oasis_open.docs.wsn.b_2.UnsupportedPolicyRequestFaultType unsupportedPolicyRequestFault;

    public UnsupportedPolicyRequestFault() {
        super();
    }
    
    public UnsupportedPolicyRequestFault(String message) {
        super(message);
    }
    
    public UnsupportedPolicyRequestFault(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportedPolicyRequestFault(String message, org.oasis_open.docs.wsn.b_2.UnsupportedPolicyRequestFaultType unsupportedPolicyRequestFault) {
        super(message);
        this.unsupportedPolicyRequestFault = unsupportedPolicyRequestFault;
    }

    public UnsupportedPolicyRequestFault(String message, org.oasis_open.docs.wsn.b_2.UnsupportedPolicyRequestFaultType unsupportedPolicyRequestFault, Throwable cause) {
        super(message, cause);
        this.unsupportedPolicyRequestFault = unsupportedPolicyRequestFault;
    }

    public org.oasis_open.docs.wsn.b_2.UnsupportedPolicyRequestFaultType getFaultInfo() {
        return this.unsupportedPolicyRequestFault;
    }
}
