
package org.ggf.namespaces._2005._12.ws_dair;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import org.ggf.namespaces._2005._12.ws_dai.DatasetType;


/**
 * <p>Java class for SQLDatasetType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SQLDatasetType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DatasetType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SQLUpdateCount" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SQLOutputParameter" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SQLReturnValue" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SQLCommunicationsArea" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SQLDatasetType", propOrder = {
    "sqlUpdateCount",
    "sqlOutputParameter",
    "sqlReturnValue",
    "sqlCommunicationsArea"
})
public class SQLDatasetType
    extends DatasetType
{

    @XmlElement(name = "SQLUpdateCount", type = Integer.class)
    protected List<Integer> sqlUpdateCount;
    @XmlElement(name = "SQLOutputParameter")
    protected List<SQLOutputParameterType> sqlOutputParameter;
    @XmlElement(name = "SQLReturnValue")
    protected String sqlReturnValue;
    @XmlElement(name = "SQLCommunicationsArea")
    protected List<SQLCommunicationsAreaType> sqlCommunicationsArea;

    /**
     * Gets the value of the sqlUpdateCount property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the sqlUpdateCount property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getSQLUpdateCount().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Integer }
     * 
     * 
     */
    public List<Integer> getSQLUpdateCount() {
        if (sqlUpdateCount == null) {
            sqlUpdateCount = new ArrayList<Integer>();
        }
        return this.sqlUpdateCount;
    }

    /**
     * Gets the value of the sqlOutputParameter property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the sqlOutputParameter property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getSQLOutputParameter().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link SQLOutputParameterType }
     * 
     * 
     */
    public List<SQLOutputParameterType> getSQLOutputParameter() {
        if (sqlOutputParameter == null) {
            sqlOutputParameter = new ArrayList<SQLOutputParameterType>();
        }
        return this.sqlOutputParameter;
    }

    /**
     * Gets the value of the sqlReturnValue property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSQLReturnValue() {
        return sqlReturnValue;
    }

    /**
     * Sets the value of the sqlReturnValue property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSQLReturnValue(String value) {
        this.sqlReturnValue = value;
    }

    /**
     * Gets the value of the sqlCommunicationsArea property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the sqlCommunicationsArea property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getSQLCommunicationsArea().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link SQLCommunicationsAreaType }
     * 
     * 
     */
    public List<SQLCommunicationsAreaType> getSQLCommunicationsArea() {
        if (sqlCommunicationsArea == null) {
            sqlCommunicationsArea = new ArrayList<SQLCommunicationsAreaType>();
        }
        return this.sqlCommunicationsArea;
    }

}
