
package org.ggf.namespaces._2005._12.ws_dai;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;
import javax.xml.namespace.QName;


/**
 * <p>Java class for DatasetMapType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DatasetMapType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}MessageQName"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DatasetFormatURI"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DatasetMapType", propOrder = {
    "messageQName",
    "datasetFormatURI"
})
public class DatasetMapType {

    @XmlElement(name = "MessageQName", required = true)
    protected QName messageQName;
    @XmlElement(name = "DatasetFormatURI", required = true)
    @XmlSchemaType(name = "anyURI")
    protected String datasetFormatURI;

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
     * Gets the value of the datasetFormatURI property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDatasetFormatURI() {
        return datasetFormatURI;
    }

    /**
     * Sets the value of the datasetFormatURI property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDatasetFormatURI(String value) {
        this.datasetFormatURI = value;
    }

}
