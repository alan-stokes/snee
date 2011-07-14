
package eu.semsorgrid4env.service.wsdai;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;
import org.apache.cxf.ws.addressing.EndpointReferenceType;


/**
 * <p>Java class for DataResourceAddressType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DataResourceAddressType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.w3.org/2005/08/addressing}EndpointReferenceType">
 *       &lt;anyAttribute processContents='lax' namespace='##other'/>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DataResourceAddressType")
public class DataResourceAddressType
    extends EndpointReferenceType
{


}
