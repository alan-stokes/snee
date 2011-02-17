
package eu.semsorgrid4env.service.wsdai;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DatasetType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DatasetType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DatasetFormatURI"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DatasetData"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DatasetType", propOrder = {
    "datasetFormatURI",
    "datasetData"
})
public class DatasetType {

    @XmlElement(name = "DatasetFormatURI", required = true)
    @XmlSchemaType(name = "anyURI")
    protected String datasetFormatURI;
    @XmlElement(name = "DatasetData", required = true)
    protected DatasetDataType datasetData;

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

    /**
     * Gets the value of the datasetData property.
     * 
     * @return
     *     possible object is
     *     {@link DatasetDataType }
     *     
     */
    public DatasetDataType getDatasetData() {
        return datasetData;
    }

    /**
     * Sets the value of the datasetData property.
     * 
     * @param value
     *     allowed object is
     *     {@link DatasetDataType }
     *     
     */
    public void setDatasetData(DatasetDataType value) {
        this.datasetData = value;
    }

}
