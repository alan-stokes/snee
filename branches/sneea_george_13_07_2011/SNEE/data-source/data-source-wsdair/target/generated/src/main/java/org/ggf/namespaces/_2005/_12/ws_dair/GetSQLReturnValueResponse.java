
package org.ggf.namespaces._2005._12.ws_dair;

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
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="ReturnValue" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "returnValue"
})
@XmlRootElement(name = "GetSQLReturnValueResponse")
public class GetSQLReturnValueResponse {

    @XmlElement(name = "ReturnValue")
    protected String returnValue;

    /**
     * Gets the value of the returnValue property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getReturnValue() {
        return returnValue;
    }

    /**
     * Sets the value of the returnValue property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setReturnValue(String value) {
        this.returnValue = value;
    }

}
