
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.ggf.namespaces._2005._12.ws_dai.DatasetType;


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
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}Dataset"/>
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
    "dataset"
})
@XmlRootElement(name = "GetSQLRowsetResponse")
public class GetSQLRowsetResponse {

    @XmlElementRef(name = "Dataset", namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", type = JAXBElement.class)
    protected JAXBElement<? extends DatasetType> dataset;

    /**
     * Gets the value of the dataset property.
     * 
     * @return
     *     possible object is
     *     {@link JAXBElement }{@code <}{@link SQLDatasetType }{@code >}
     *     {@link JAXBElement }{@code <}{@link DatasetType }{@code >}
     *     
     */
    public JAXBElement<? extends DatasetType> getDataset() {
        return dataset;
    }

    /**
     * Sets the value of the dataset property.
     * 
     * @param value
     *     allowed object is
     *     {@link JAXBElement }{@code <}{@link SQLDatasetType }{@code >}
     *     {@link JAXBElement }{@code <}{@link DatasetType }{@code >}
     *     
     */
    public void setDataset(JAXBElement<? extends DatasetType> value) {
        this.dataset = ((JAXBElement<? extends DatasetType> ) value);
    }

}
