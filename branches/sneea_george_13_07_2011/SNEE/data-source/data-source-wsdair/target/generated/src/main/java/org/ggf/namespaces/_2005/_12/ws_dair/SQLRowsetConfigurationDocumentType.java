
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.ggf.namespaces._2005._12.ws_dai.ConfigurationDocumentType;


/**
 * <p>Java class for SQLRowsetConfigurationDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SQLRowsetConfigurationDocumentType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ConfigurationDocumentType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}AccessMode" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SQLRowsetConfigurationDocumentType", propOrder = {
    "accessMode"
})
public class SQLRowsetConfigurationDocumentType
    extends ConfigurationDocumentType
{

    @XmlElement(name = "AccessMode")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String accessMode;

    /**
     * Gets the value of the accessMode property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getAccessMode() {
        return accessMode;
    }

    /**
     * Sets the value of the accessMode property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setAccessMode(String value) {
        this.accessMode = value;
    }

}
