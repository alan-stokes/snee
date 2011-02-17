
package eu.semsorgrid4env.service.stream.query;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import eu.semsorgrid4env.service.wsdai.ExpressionType;


/**
 * <p>Java class for StreamQueryExpressionType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="StreamQueryExpressionType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ExpressionType">
 *       &lt;sequence>
 *         &lt;element name="Expression" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="QueryDuration" type="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS}AbsoluteOrRelativeTimeType"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "StreamQueryExpressionType", propOrder = {
    "expression",
    "queryDuration"
})
public class StreamQueryExpressionType
    extends ExpressionType
{

    @XmlElement(name = "Expression", required = true)
    protected String expression;
    @XmlElement(name = "QueryDuration", required = true)
    protected String queryDuration;

    /**
     * Gets the value of the expression property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getExpression() {
        return expression;
    }

    /**
     * Sets the value of the expression property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setExpression(String value) {
        this.expression = value;
    }

    /**
     * Gets the value of the queryDuration property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getQueryDuration() {
        return queryDuration;
    }

    /**
     * Sets the value of the queryDuration property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setQueryDuration(String value) {
        this.queryDuration = value;
    }

}
