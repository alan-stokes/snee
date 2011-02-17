
package eu.semsorgrid4env.service.stream.push;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for PublicationPolicyType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="PublicationPolicyType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push}MaximumNumberItems" minOccurs="0"/>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push}SupportRawMessages"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PublicationPolicyType", propOrder = {
    "maximumNumberItems",
    "supportRawMessages"
})
public class PublicationPolicyType {

    @XmlElement(name = "MaximumNumberItems", defaultValue = "1")
    protected Integer maximumNumberItems;
    @XmlElement(name = "SupportRawMessages", defaultValue = "false")
    protected boolean supportRawMessages;

    /**
     * Gets the value of the maximumNumberItems property.
     * 
     * @return
     *     possible object is
     *     {@link Integer }
     *     
     */
    public Integer getMaximumNumberItems() {
        return maximumNumberItems;
    }

    /**
     * Sets the value of the maximumNumberItems property.
     * 
     * @param value
     *     allowed object is
     *     {@link Integer }
     *     
     */
    public void setMaximumNumberItems(Integer value) {
        this.maximumNumberItems = value;
    }

    /**
     * Gets the value of the supportRawMessages property.
     * 
     */
    public boolean isSupportRawMessages() {
        return supportRawMessages;
    }

    /**
     * Sets the value of the supportRawMessages property.
     * 
     */
    public void setSupportRawMessages(boolean value) {
        this.supportRawMessages = value;
    }

}
