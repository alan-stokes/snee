
package eu.semsorgrid4env.service.wsdai;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}RequestType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}GenericExpression"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "genericExpression"
})
@XmlRootElement(name = "GenericQueryRequest")
public class GenericQueryRequest
    extends RequestType
{

    @XmlElement(name = "GenericExpression", required = true)
    protected GenericExpression genericExpression;

    /**
     * Gets the value of the genericExpression property.
     * 
     * @return
     *     possible object is
     *     {@link GenericExpression }
     *     
     */
    public GenericExpression getGenericExpression() {
        return genericExpression;
    }

    /**
     * Sets the value of the genericExpression property.
     * 
     * @param value
     *     allowed object is
     *     {@link GenericExpression }
     *     
     */
    public void setGenericExpression(GenericExpression value) {
        this.genericExpression = value;
    }

}
