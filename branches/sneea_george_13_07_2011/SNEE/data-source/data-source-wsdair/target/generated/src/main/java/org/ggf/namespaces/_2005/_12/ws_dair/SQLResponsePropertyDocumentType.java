
package org.ggf.namespaces._2005._12.ws_dair;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;
import org.ggf.namespaces._2005._12.ws_dai.PropertyDocumentType;


/**
 * <p>Java class for SQLResponsePropertyDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SQLResponsePropertyDocumentType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}PropertyDocumentType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SQLResponseItem" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}NumberOfSQLRowsets"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}NumberOfSQLUpdateCounts"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}NumberOfSQLReturnValues"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}NumberOfSQLOutputParameters"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}NumberOfSQLCommunicationsAreas"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SQLResponsePropertyDocumentType", propOrder = {
    "sqlResponseItem",
    "numberOfSQLRowsets",
    "numberOfSQLUpdateCounts",
    "numberOfSQLReturnValues",
    "numberOfSQLOutputParameters",
    "numberOfSQLCommunicationsAreas"
})
public class SQLResponsePropertyDocumentType
    extends PropertyDocumentType
{

    @XmlElement(name = "SQLResponseItem")
    protected List<SQLResponseItemType> sqlResponseItem;
    @XmlElement(name = "NumberOfSQLRowsets")
    @XmlSchemaType(name = "unsignedInt")
    protected long numberOfSQLRowsets;
    @XmlElement(name = "NumberOfSQLUpdateCounts")
    @XmlSchemaType(name = "unsignedInt")
    protected long numberOfSQLUpdateCounts;
    @XmlElement(name = "NumberOfSQLReturnValues")
    @XmlSchemaType(name = "unsignedInt")
    protected long numberOfSQLReturnValues;
    @XmlElement(name = "NumberOfSQLOutputParameters")
    @XmlSchemaType(name = "unsignedInt")
    protected long numberOfSQLOutputParameters;
    @XmlElement(name = "NumberOfSQLCommunicationsAreas")
    @XmlSchemaType(name = "unsignedInt")
    protected long numberOfSQLCommunicationsAreas;

    /**
     * Gets the value of the sqlResponseItem property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the sqlResponseItem property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getSQLResponseItem().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link SQLResponseItemType }
     * 
     * 
     */
    public List<SQLResponseItemType> getSQLResponseItem() {
        if (sqlResponseItem == null) {
            sqlResponseItem = new ArrayList<SQLResponseItemType>();
        }
        return this.sqlResponseItem;
    }

    /**
     * Gets the value of the numberOfSQLRowsets property.
     * 
     */
    public long getNumberOfSQLRowsets() {
        return numberOfSQLRowsets;
    }

    /**
     * Sets the value of the numberOfSQLRowsets property.
     * 
     */
    public void setNumberOfSQLRowsets(long value) {
        this.numberOfSQLRowsets = value;
    }

    /**
     * Gets the value of the numberOfSQLUpdateCounts property.
     * 
     */
    public long getNumberOfSQLUpdateCounts() {
        return numberOfSQLUpdateCounts;
    }

    /**
     * Sets the value of the numberOfSQLUpdateCounts property.
     * 
     */
    public void setNumberOfSQLUpdateCounts(long value) {
        this.numberOfSQLUpdateCounts = value;
    }

    /**
     * Gets the value of the numberOfSQLReturnValues property.
     * 
     */
    public long getNumberOfSQLReturnValues() {
        return numberOfSQLReturnValues;
    }

    /**
     * Sets the value of the numberOfSQLReturnValues property.
     * 
     */
    public void setNumberOfSQLReturnValues(long value) {
        this.numberOfSQLReturnValues = value;
    }

    /**
     * Gets the value of the numberOfSQLOutputParameters property.
     * 
     */
    public long getNumberOfSQLOutputParameters() {
        return numberOfSQLOutputParameters;
    }

    /**
     * Sets the value of the numberOfSQLOutputParameters property.
     * 
     */
    public void setNumberOfSQLOutputParameters(long value) {
        this.numberOfSQLOutputParameters = value;
    }

    /**
     * Gets the value of the numberOfSQLCommunicationsAreas property.
     * 
     */
    public long getNumberOfSQLCommunicationsAreas() {
        return numberOfSQLCommunicationsAreas;
    }

    /**
     * Sets the value of the numberOfSQLCommunicationsAreas property.
     * 
     */
    public void setNumberOfSQLCommunicationsAreas(long value) {
        this.numberOfSQLCommunicationsAreas = value;
    }

}
