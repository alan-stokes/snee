
package eu.semsorgrid4env.service.wsdai;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import eu.semsorgrid4env.service.stream.StreamPropertyDocumentType;


/**
 * <p>Java class for PropertyDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="PropertyDocumentType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DataResourceAbstractName"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DataResourceManagement"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ParentDataResource" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DatasetMap" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ConfigurationMap" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}LanguageMap" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DataResourceDescription"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}Readable"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}Writeable"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ConcurrentAccess"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}TransactionInitiation"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}TransactionIsolation"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ChildSensitiveToParent"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ParentSensitiveToChild"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PropertyDocumentType", propOrder = {
    "dataResourceAbstractName",
    "dataResourceManagement",
    "parentDataResource",
    "datasetMap",
    "configurationMap",
    "languageMap",
    "dataResourceDescription",
    "readable",
    "writeable",
    "concurrentAccess",
    "transactionInitiation",
    "transactionIsolation",
    "childSensitiveToParent",
    "parentSensitiveToChild"
})
@XmlSeeAlso({
    StreamPropertyDocumentType.class
})
public class PropertyDocumentType {

    @XmlElement(name = "DataResourceAbstractName", required = true)
    protected String dataResourceAbstractName;
    @XmlElement(name = "DataResourceManagement", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String dataResourceManagement;
    @XmlElement(name = "ParentDataResource")
    protected DataResourceAddressType parentDataResource;
    @XmlElement(name = "DatasetMap")
    protected List<DatasetMapType> datasetMap;
    @XmlElement(name = "ConfigurationMap")
    protected List<ConfigurationMapType> configurationMap;
    @XmlElement(name = "LanguageMap")
    protected List<LanguageMapType> languageMap;
    @XmlElement(name = "DataResourceDescription", required = true)
    protected DataResourceDescription dataResourceDescription;
    @XmlElement(name = "Readable", defaultValue = "true")
    protected boolean readable;
    @XmlElement(name = "Writeable")
    protected boolean writeable;
    @XmlElement(name = "ConcurrentAccess")
    protected boolean concurrentAccess;
    @XmlElement(name = "TransactionInitiation", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String transactionInitiation;
    @XmlElement(name = "TransactionIsolation", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String transactionIsolation;
    @XmlElement(name = "ChildSensitiveToParent", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String childSensitiveToParent;
    @XmlElement(name = "ParentSensitiveToChild", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String parentSensitiveToChild;

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

    /**
     * Gets the value of the dataResourceManagement property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDataResourceManagement() {
        return dataResourceManagement;
    }

    /**
     * Sets the value of the dataResourceManagement property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDataResourceManagement(String value) {
        this.dataResourceManagement = value;
    }

    /**
     * Gets the value of the parentDataResource property.
     * 
     * @return
     *     possible object is
     *     {@link DataResourceAddressType }
     *     
     */
    public DataResourceAddressType getParentDataResource() {
        return parentDataResource;
    }

    /**
     * Sets the value of the parentDataResource property.
     * 
     * @param value
     *     allowed object is
     *     {@link DataResourceAddressType }
     *     
     */
    public void setParentDataResource(DataResourceAddressType value) {
        this.parentDataResource = value;
    }

    /**
     * Gets the value of the datasetMap property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the datasetMap property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDatasetMap().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DatasetMapType }
     * 
     * 
     */
    public List<DatasetMapType> getDatasetMap() {
        if (datasetMap == null) {
            datasetMap = new ArrayList<DatasetMapType>();
        }
        return this.datasetMap;
    }

    /**
     * Gets the value of the configurationMap property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the configurationMap property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getConfigurationMap().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ConfigurationMapType }
     * 
     * 
     */
    public List<ConfigurationMapType> getConfigurationMap() {
        if (configurationMap == null) {
            configurationMap = new ArrayList<ConfigurationMapType>();
        }
        return this.configurationMap;
    }

    /**
     * Gets the value of the languageMap property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the languageMap property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getLanguageMap().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link LanguageMapType }
     * 
     * 
     */
    public List<LanguageMapType> getLanguageMap() {
        if (languageMap == null) {
            languageMap = new ArrayList<LanguageMapType>();
        }
        return this.languageMap;
    }

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
     */
    public boolean isReadable() {
        return readable;
    }

    /**
     * Sets the value of the readable property.
     * 
     */
    public void setReadable(boolean value) {
        this.readable = value;
    }

    /**
     * Gets the value of the writeable property.
     * 
     */
    public boolean isWriteable() {
        return writeable;
    }

    /**
     * Sets the value of the writeable property.
     * 
     */
    public void setWriteable(boolean value) {
        this.writeable = value;
    }

    /**
     * Gets the value of the concurrentAccess property.
     * 
     */
    public boolean isConcurrentAccess() {
        return concurrentAccess;
    }

    /**
     * Sets the value of the concurrentAccess property.
     * 
     */
    public void setConcurrentAccess(boolean value) {
        this.concurrentAccess = value;
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
