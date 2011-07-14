
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;


/**
 * <p>Java class for SQLResponseItemType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SQLResponseItemType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="SequenceNumber" type="{http://www.w3.org/2001/XMLSchema}unsignedInt"/>
 *         &lt;element name="Name">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}token">
 *               &lt;enumeration value="Rowset"/>
 *               &lt;enumeration value="UpdateCount"/>
 *               &lt;enumeration value="ReturnValue"/>
 *               &lt;enumeration value="OutputParameter"/>
 *               &lt;enumeration value="CommunicationsArea"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
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
@XmlType(name = "SQLResponseItemType", propOrder = {
    "sequenceNumber",
    "name"
})
public class SQLResponseItemType {

    @XmlElement(name = "SequenceNumber", namespace = "")
    @XmlSchemaType(name = "unsignedInt")
    protected long sequenceNumber;
    @XmlElement(name = "Name", namespace = "", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String name;

    /**
     * Gets the value of the sequenceNumber property.
     * 
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Sets the value of the sequenceNumber property.
     * 
     */
    public void setSequenceNumber(long value) {
        this.sequenceNumber = value;
    }

    /**
     * Gets the value of the name property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the value of the name property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setName(String value) {
        this.name = value;
    }

}
