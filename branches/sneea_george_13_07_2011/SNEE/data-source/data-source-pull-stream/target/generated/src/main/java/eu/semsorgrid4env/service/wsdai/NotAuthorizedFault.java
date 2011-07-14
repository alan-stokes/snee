
package eu.semsorgrid4env.service.wsdai;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Thu Jul 14 09:06:21 EEST 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "NotAuthorizedFault", targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI")
public class NotAuthorizedFault extends Exception {
    public static final long serialVersionUID = 20110714090621L;
    
    private eu.semsorgrid4env.service.wsdai.NotAuthorizedFaultType notAuthorizedFault;

    public NotAuthorizedFault() {
        super();
    }
    
    public NotAuthorizedFault(String message) {
        super(message);
    }
    
    public NotAuthorizedFault(String message, Throwable cause) {
        super(message, cause);
    }

    public NotAuthorizedFault(String message, eu.semsorgrid4env.service.wsdai.NotAuthorizedFaultType notAuthorizedFault) {
        super(message);
        this.notAuthorizedFault = notAuthorizedFault;
    }

    public NotAuthorizedFault(String message, eu.semsorgrid4env.service.wsdai.NotAuthorizedFaultType notAuthorizedFault, Throwable cause) {
        super(message, cause);
        this.notAuthorizedFault = notAuthorizedFault;
    }

    public eu.semsorgrid4env.service.wsdai.NotAuthorizedFaultType getFaultInfo() {
        return this.notAuthorizedFault;
    }
}
