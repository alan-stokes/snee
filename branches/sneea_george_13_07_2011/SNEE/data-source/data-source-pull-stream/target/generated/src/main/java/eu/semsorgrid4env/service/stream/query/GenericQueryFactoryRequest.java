
package eu.semsorgrid4env.service.stream.query;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import eu.semsorgrid4env.service.wsdai.ExpressionType;
import eu.semsorgrid4env.service.wsdai.FactoryRequestType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}FactoryRequestType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}Expression"/>
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
    "expression"
})
@XmlRootElement(name = "GenericQueryFactoryRequest")
public class GenericQueryFactoryRequest
    extends FactoryRequestType
{

    @XmlElement(name = "Expression", namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", required = true)
    protected ExpressionType expression;

    /**
     * Gets the value of the expression property.
     * 
     * @return
     *     possible object is
     *     {@link ExpressionType }
     *     
     */
    public ExpressionType getExpression() {
        return expression;
    }

    /**
     * Sets the value of the expression property.
     * 
     * @param value
     *     allowed object is
     *     {@link ExpressionType }
     *     
     */
    public void setExpression(ExpressionType value) {
        this.expression = value;
    }

}
