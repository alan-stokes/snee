
package eu.semsorgrid4env.service.stream.push;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;
import javax.xml.datatype.Duration;


/**
 * <p>Java class for SubscriptionPolicyType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SubscriptionPolicyType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push}MaximumSubscriptionPeriod" minOccurs="0"/>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push}FilterLanguageURI" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push}MessageContentExpressionLanguageURI" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SubscriptionPolicyType", propOrder = {
    "maximumSubscriptionPeriod",
    "filterLanguageURI",
    "messageContentExpressionLanguageURI"
})
public class SubscriptionPolicyType {

    @XmlElement(name = "MaximumSubscriptionPeriod")
    protected Duration maximumSubscriptionPeriod;
    @XmlElement(name = "FilterLanguageURI")
    @XmlSchemaType(name = "anyURI")
    protected List<String> filterLanguageURI;
    @XmlElement(name = "MessageContentExpressionLanguageURI")
    @XmlSchemaType(name = "anyURI")
    protected List<String> messageContentExpressionLanguageURI;

    /**
     * Gets the value of the maximumSubscriptionPeriod property.
     * 
     * @return
     *     possible object is
     *     {@link Duration }
     *     
     */
    public Duration getMaximumSubscriptionPeriod() {
        return maximumSubscriptionPeriod;
    }

    /**
     * Sets the value of the maximumSubscriptionPeriod property.
     * 
     * @param value
     *     allowed object is
     *     {@link Duration }
     *     
     */
    public void setMaximumSubscriptionPeriod(Duration value) {
        this.maximumSubscriptionPeriod = value;
    }

    /**
     * Gets the value of the filterLanguageURI property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the filterLanguageURI property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getFilterLanguageURI().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getFilterLanguageURI() {
        if (filterLanguageURI == null) {
            filterLanguageURI = new ArrayList<String>();
        }
        return this.filterLanguageURI;
    }

    /**
     * Gets the value of the messageContentExpressionLanguageURI property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the messageContentExpressionLanguageURI property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getMessageContentExpressionLanguageURI().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getMessageContentExpressionLanguageURI() {
        if (messageContentExpressionLanguageURI == null) {
            messageContentExpressionLanguageURI = new ArrayList<String>();
        }
        return this.messageContentExpressionLanguageURI;
    }

}
