
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import org.ggf.namespaces._2005._12.ws_dai.PropertyDocumentType;


/**
 * <p>Java class for SQLPropertyDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SQLPropertyDocumentType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}PropertyDocumentType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SchemaDescription"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SQLPropertyDocumentType", propOrder = {
    "schemaDescription"
})
public class SQLPropertyDocumentType
    extends PropertyDocumentType
{

    @XmlElement(name = "SchemaDescription", required = true)
    protected SchemaDescription schemaDescription;

    /**
     * Gets the value of the schemaDescription property.
     * 
     * @return
     *     possible object is
     *     {@link SchemaDescription }
     *     
     */
    public SchemaDescription getSchemaDescription() {
        return schemaDescription;
    }

    /**
     * Sets the value of the schemaDescription property.
     * 
     * @param value
     *     allowed object is
     *     {@link SchemaDescription }
     *     
     */
    public void setSchemaDescription(SchemaDescription value) {
        this.schemaDescription = value;
    }

}
