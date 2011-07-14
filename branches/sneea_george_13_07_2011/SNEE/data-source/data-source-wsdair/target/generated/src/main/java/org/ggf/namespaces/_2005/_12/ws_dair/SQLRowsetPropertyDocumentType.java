
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.ggf.namespaces._2005._12.ws_dai.PropertyDocumentType;


/**
 * <p>Java class for SQLRowsetPropertyDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SQLRowsetPropertyDocumentType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}PropertyDocumentType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}RowSchema"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}NoOfRows"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}AccessMode"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SQLRowsetPropertyDocumentType", propOrder = {
    "rowSchema",
    "noOfRows",
    "accessMode"
})
public class SQLRowsetPropertyDocumentType
    extends PropertyDocumentType
{

    @XmlElement(name = "RowSchema", required = true)
    protected RowSchemaType rowSchema;
    @XmlElement(name = "NoOfRows")
    protected int noOfRows;
    @XmlElement(name = "AccessMode", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String accessMode;

    /**
     * Gets the value of the rowSchema property.
     * 
     * @return
     *     possible object is
     *     {@link RowSchemaType }
     *     
     */
    public RowSchemaType getRowSchema() {
        return rowSchema;
    }

    /**
     * Sets the value of the rowSchema property.
     * 
     * @param value
     *     allowed object is
     *     {@link RowSchemaType }
     *     
     */
    public void setRowSchema(RowSchemaType value) {
        this.rowSchema = value;
    }

    /**
     * Gets the value of the noOfRows property.
     * 
     */
    public int getNoOfRows() {
        return noOfRows;
    }

    /**
     * Sets the value of the noOfRows property.
     * 
     */
    public void setNoOfRows(int value) {
        this.noOfRows = value;
    }

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
