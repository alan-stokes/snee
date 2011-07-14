
package eu.semsorgrid4env.service.wsdai;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;


/**
 * <p>Java class for ConfigurationDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="ConfigurationDocumentType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DataResourceDescription" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}Readable" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}Writeable" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}TransactionInitiation" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}TransactionIsolation" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ChildSensitiveToParent" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ParentSensitiveToChild" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ConfigurationDocumentType", propOrder = {
    "dataResourceDescription",
    "readable",
    "writeable",
    "transactionInitiation",
    "transactionIsolation",
    "childSensitiveToParent",
    "parentSensitiveToChild"
})
public class ConfigurationDocumentType {

    @XmlElement(name = "DataResourceDescription")
    protected DataResourceDescription dataResourceDescription;
    @XmlElement(name = "Readable", defaultValue = "true")
    protected Boolean readable;
    @XmlElement(name = "Writeable")
    protected Boolean writeable;
    @XmlElement(name = "TransactionInitiation")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String transactionInitiation;
    @XmlElement(name = "TransactionIsolation")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String transactionIsolation;
    @XmlElement(name = "ChildSensitiveToParent")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String childSensitiveToParent;
    @XmlElement(name = "ParentSensitiveToChild")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String parentSensitiveToChild;

    /**
     * Gets the value of the dataResourceDescription property.
     * 
     * @return
     *     possible object is
     *     {@link DataResourceDescription }
     *     
     */
    public DataResourceDescription getDataResourceDescription() {
        return dataResourceDescription;
    }

    /**
     * Sets the value of the dataResourceDescription property.
     * 
     * @param value
     *     allowed object is
     *     {@link DataResourceDescription }
     *     
     */
    public void setDataResourceDescription(DataResourceDescription value) {
        this.dataResourceDescription = value;
    }

    /**
     * Gets the value of the readable property.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public Boolean isReadable() {
        return readable;
    }

    /**
     * Sets the value of the readable property.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setReadable(Boolean value) {
        this.readable = value;
    }

    /**
     * Gets the value of the writeable property.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public Boolean isWriteable() {
        return writeable;
    }

    /**
     * Sets the value of the writeable property.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setWriteable(Boolean value) {
        this.writeable = value;
    }

    /**
     * Gets the value of the transactionInitiation property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTransactionInitiation() {
        return transactionInitiation;
    }

    /**
     * Sets the value of the transactionInitiation property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTransactionInitiation(String value) {
        this.transactionInitiation = value;
    }

    /**
     * Gets the value of the transactionIsolation property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTransactionIsolation() {
        return transactionIsolation;
    }

    /**
     * Sets the value of the transactionIsolation property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTransactionIsolation(String value) {
        this.transactionIsolation = value;
    }

    /**
     * Gets the value of the childSensitiveToParent property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getChildSensitiveToParent() {
        return childSensitiveToParent;
    }

    /**
     * Sets the value of the childSensitiveToParent property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setChildSensitiveToParent(String value) {
        this.childSensitiveToParent = value;
    }

    /**
     * Gets the value of the parentSensitiveToChild property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getParentSensitiveToChild() {
        return parentSensitiveToChild;
    }

    /**
     * Sets the value of the parentSensitiveToChild property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setParentSensitiveToChild(String value) {
        this.parentSensitiveToChild = value;
    }

}
