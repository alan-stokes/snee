
package org.ggf.namespaces._2005._12.ws_dair;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import org.ggf.namespaces._2005._12.ws_dai.ExpressionType;


/**
 * <p>Java class for SQLExpressionType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SQLExpressionType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ExpressionType">
 *       &lt;sequence>
 *         &lt;element name="Expression" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="SQLParameter" type="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SQLParameterType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SQLExpressionType", propOrder = {
    "expression",
    "sqlParameter"
})
public class SQLExpressionType
    extends ExpressionType
{

    @XmlElement(name = "Expression", required = true)
    protected String expression;
    @XmlElement(name = "SQLParameter")
    protected List<SQLParameterType> sqlParameter;

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
     * Gets the value of the sqlParameter property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the sqlParameter property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getSQLParameter().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link SQLParameterType }
     * 
     * 
     */
    public List<SQLParameterType> getSQLParameter() {
        if (sqlParameter == null) {
            sqlParameter = new ArrayList<SQLParameterType>();
        }
        return this.sqlParameter;
    }

}
