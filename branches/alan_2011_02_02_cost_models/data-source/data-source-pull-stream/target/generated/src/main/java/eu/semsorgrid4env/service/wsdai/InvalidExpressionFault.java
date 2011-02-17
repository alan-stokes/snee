
package eu.semsorgrid4env.service.wsdai;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.2.8
 * Fri Feb 04 16:27:48 GMT 2011
 * Generated source version: 2.2.8
 * 
 */

@WebFault(name = "InvalidExpressionFault", targetNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI")
public class InvalidExpressionFault extends Exception {
    public static final long serialVersionUID = 20110204162748L;
    
    private eu.semsorgrid4env.service.wsdai.InvalidExpressionFaultType invalidExpressionFault;

    public InvalidExpressionFault() {
        super();
    }
    
    public InvalidExpressionFault(String message) {
        super(message);
    }
    
    public InvalidExpressionFault(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidExpressionFault(String message, eu.semsorgrid4env.service.wsdai.InvalidExpressionFaultType invalidExpressionFault) {
        super(message);
        this.invalidExpressionFault = invalidExpressionFault;
    }

    public InvalidExpressionFault(String message, eu.semsorgrid4env.service.wsdai.InvalidExpressionFaultType invalidExpressionFault, Throwable cause) {
        super(message, cause);
        this.invalidExpressionFault = invalidExpressionFault;
    }

    public eu.semsorgrid4env.service.wsdai.InvalidExpressionFaultType getFaultInfo() {
        return this.invalidExpressionFault;
    }
}
