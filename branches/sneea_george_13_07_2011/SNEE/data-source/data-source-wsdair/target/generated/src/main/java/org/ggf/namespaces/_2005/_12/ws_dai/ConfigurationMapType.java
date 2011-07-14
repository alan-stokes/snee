
package org.ggf.namespaces._2005._12.ws_dai;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlMixed;
import javax.xml.bind.annotation.XmlType;
import javax.xml.namespace.QName;
import org.ggf.namespaces._2005._12.ws_dair.SQLRowsetConfigurationDocumentType;


/**
 * <p>Java class for ConfigurationMapType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="ConfigurationMapType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}MessageQName"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}PortTypeQName"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ConfigurationDocumentQName"/>
 *         &lt;element name="DefaultConfigurationDocument">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ConfigurationDocument"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ConfigurationMapType", propOrder = {
    "messageQName",
    "portTypeQName",
    "configurationDocumentQName",
    "defaultConfigurationDocument"
})
public class ConfigurationMapType {

    @XmlElement(name = "MessageQName", required = true)
    protected QName messageQName;
    @XmlElement(name = "PortTypeQName", required = true)
    protected QName portTypeQName;
    @XmlElement(name = "ConfigurationDocumentQName", required = true)
    protected QName configurationDocumentQName;
    @XmlElement(name = "DefaultConfigurationDocument", namespace = "", required = true)
    protected ConfigurationMapType.DefaultConfigurationDocument defaultConfigurationDocument;

    /**
     * Gets the value of the messageQName property.
     * 
     * @return
     *     possible object is
     *     {@link QName }
     *     
     */
    public QName getMessageQName() {
        return messageQName;
    }

    /**
     * Sets the value of the messageQName property.
     * 
     * @param value
     *     allowed object is
     *     {@link QName }
     *     
     */
    public void setMessageQName(QName value) {
        this.messageQName = value;
    }

    /**
     * Gets the value of the portTypeQName property.
     * 
     * @return
     *     possible object is
     *     {@link QName }
     *     
     */
    public QName getPortTypeQName() {
        return portTypeQName;
    }

    /**
     * Sets the value of the portTypeQName property.
     * 
     * @param value
     *     allowed object is
     *     {@link QName }
     *     
     */
    public void setPortTypeQName(QName value) {
        this.portTypeQName = value;
    }

    /**
     * Gets the value of the configurationDocumentQName property.
     * 
     * @return
     *     possible object is
     *     {@link QName }
     *     
     */
    public QName getConfigurationDocumentQName() {
        return configurationDocumentQName;
    }

    /**
     * Sets the value of the configurationDocumentQName property.
     * 
     * @param value
     *     allowed object is
     *     {@link QName }
     *     
     */
    public void setConfigurationDocumentQName(QName value) {
        this.configurationDocumentQName = value;
    }

    /**
     * Gets the value of the defaultConfigurationDocument property.
     * 
     * @return
     *     possible object is
     *     {@link ConfigurationMapType.DefaultConfigurationDocument }
     *     
     */
    public ConfigurationMapType.DefaultConfigurationDocument getDefaultConfigurationDocument() {
        return defaultConfigurationDocument;
    }

    /**
     * Sets the value of the defaultConfigurationDocument property.
     * 
     * @param value
     *     allowed object is
     *     {@link ConfigurationMapType.DefaultConfigurationDocument }
     *     
     */
    public void setDefaultConfigurationDocument(ConfigurationMapType.DefaultConfigurationDocument value) {
        this.defaultConfigurationDocument = value;
    }


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
     *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ConfigurationDocument"/>
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
        "content"
    })
    public static class DefaultConfigurationDocument {

        @XmlElementRef(name = "ConfigurationDocument", namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", type = JAXBElement.class)
        @XmlMixed
        protected List<Serializable> content;

        /**
         * Gets the value of the content property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the content property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getContent().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link JAXBElement }{@code <}{@link ConfigurationDocumentType }{@code >}
         * {@link String }
         * {@link JAXBElement }{@code <}{@link SQLRowsetConfigurationDocumentType }{@code >}
         * 
         * 
         */
        public List<Serializable> getContent() {
            if (content == null) {
                content = new ArrayList<Serializable>();
            }
            return this.content;
        }

    }

}
