
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.ggf.namespaces._2005._12.ws_dai.RequestType;


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
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SQLExpression"/>
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
    "sqlExpression"
})
@XmlRootElement(name = "SQLExecuteRequest")
public class SQLExecuteRequest
    extends RequestType
{

    @XmlElement(name = "SQLExpression", required = true)
    protected SQLExpressionType sqlExpression;

    /**
     * Gets the value of the sqlExpression property.
     * 
     * @return
     *     possible object is
     *     {@link SQLExpressionType }
     *     
     */
    public SQLExpressionType getSQLExpression() {
        return sqlExpression;
    }

    /**
     * Sets the value of the sqlExpression property.
     * 
     * @param value
     *     allowed object is
     *     {@link SQLExpressionType }
     *     
     */
    public void setSQLExpression(SQLExpressionType value) {
        this.sqlExpression = value;
    }

}
