
package org.ggf.namespaces._2005._12.ws_dai;

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
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DataResourceAbstractName"/>
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
    "dataResourceAbstractName"
})
@XmlRootElement(name = "ResolveRequest")
public class ResolveRequest {

    @XmlElement(name = "DataResourceAbstractName", required = true)
    protected String dataResourceAbstractName;

    /**
     * Gets the value of the dataResourceAbstractName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDataResourceAbstractName() {
        return dataResourceAbstractName;
    }

    /**
     * Sets the value of the dataResourceAbstractName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDataResourceAbstractName(String value) {
        this.dataResourceAbstractName = value;
    }

}
