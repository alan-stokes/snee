
package org.ggf.namespaces._2005._12.ws_dai;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;
import org.ggf.namespaces._2005._12.ws_dair.GetTuplesRequest;


/**
 * <p>Java class for RequestType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="RequestType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}BaseRequestType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DatasetFormatURI" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "RequestType", propOrder = {
    "datasetFormatURI"
})
@XmlSeeAlso({
    GetTuplesRequest.class,
    GenericQueryRequest.class,
    GetDataResourcePropertyDocumentRequest.class
})
public class RequestType
    extends BaseRequestType
{

    @XmlElement(name = "DatasetFormatURI")
    @XmlSchemaType(name = "anyURI")
    protected String datasetFormatURI;

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

}
