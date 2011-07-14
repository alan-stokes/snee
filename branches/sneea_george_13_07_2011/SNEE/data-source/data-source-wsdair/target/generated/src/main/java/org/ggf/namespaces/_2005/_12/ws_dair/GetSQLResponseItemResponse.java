
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


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
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAIR}SQLDataset"/>
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
    "sqlDataset"
})
@XmlRootElement(name = "GetSQLResponseItemResponse")
public class GetSQLResponseItemResponse {

    @XmlElement(name = "SQLDataset", required = true)
    protected SQLDatasetType sqlDataset;

    /**
     * Gets the value of the sqlDataset property.
     * 
     * @return
     *     possible object is
     *     {@link SQLDatasetType }
     *     
     */
    public SQLDatasetType getSQLDataset() {
        return sqlDataset;
    }

    /**
     * Sets the value of the sqlDataset property.
     * 
     * @param value
     *     allowed object is
     *     {@link SQLDatasetType }
     *     
     */
    public void setSQLDataset(SQLDatasetType value) {
        this.sqlDataset = value;
    }

}
