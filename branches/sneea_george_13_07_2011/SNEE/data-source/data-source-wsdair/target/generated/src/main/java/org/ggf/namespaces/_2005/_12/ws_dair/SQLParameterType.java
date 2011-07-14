
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;


/**
 * <p>Java class for SQLParameterType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SQLParameterType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="Value" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="Type">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}token">
 *               &lt;enumeration value="BIT"/>
 *               &lt;enumeration value="TINYINT"/>
 *               &lt;enumeration value="SMALLINT"/>
 *               &lt;enumeration value="INTEGER"/>
 *               &lt;enumeration value="BIGINT"/>
 *               &lt;enumeration value="FLOAT"/>
 *               &lt;enumeration value="REAL"/>
 *               &lt;enumeration value="DOUBLE"/>
 *               &lt;enumeration value="NUMERIC"/>
 *               &lt;enumeration value="DECIMAL"/>
 *               &lt;enumeration value="CHAR"/>
 *               &lt;enumeration value="VARCHAR"/>
 *               &lt;enumeration value="LONGVARCHAR"/>
 *               &lt;enumeration value="DATE"/>
 *               &lt;enumeration value="TIME"/>
 *               &lt;enumeration value="TIMESTAMP"/>
 *               &lt;enumeration value="BINARY"/>
 *               &lt;enumeration value="VARBINARY"/>
 *               &lt;enumeration value="LONGVARBINARY"/>
 *               &lt;enumeration value="NULL"/>
 *               &lt;enumeration value="DISTINCT"/>
 *               &lt;enumeration value="STRUCT"/>
 *               &lt;enumeration value="ARRAY"/>
 *               &lt;enumeration value="BLOB"/>
 *               &lt;enumeration value="CLOB"/>
 *               &lt;enumeration value="REF"/>
 *               &lt;enumeration value="DATALINK"/>
 *               &lt;enumeration value="BOOLEAN"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *         &lt;element name="Mode">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}token">
 *               &lt;enumeration value="IN"/>
 *               &lt;enumeration value="OUT"/>
 *               &lt;enumeration value="INOUT"/>
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
@XmlType(name = "SQLParameterType", propOrder = {
    "value",
    "type",
    "mode"
})
public class SQLParameterType {

    @XmlElement(name = "Value", required = true)
    protected String value;
    @XmlElement(name = "Type", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String type;
    @XmlElement(name = "Mode", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String mode;

    /**
     * Gets the value of the value property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the value of the value property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Gets the value of the type property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the value of the type property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setType(String value) {
        this.type = value;
    }

    /**
     * Gets the value of the mode property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMode() {
        return mode;
    }

    /**
     * Sets the value of the mode property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMode(String value) {
        this.mode = value;
    }

}
