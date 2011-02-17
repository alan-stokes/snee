
package eu.semsorgrid4env.service.stream;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.namespace.QName;


/**
 * <p>Java class for StreamRateType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="StreamRateType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="StreamQName" type="{http://www.w3.org/2001/XMLSchema}QName"/>
 *         &lt;element name="MinimumFlowRate" type="{http://www.w3.org/2001/XMLSchema}double" minOccurs="0"/>
 *         &lt;element name="AverageFlowRate" type="{http://www.w3.org/2001/XMLSchema}double" minOccurs="0"/>
 *         &lt;element name="MaximumFlowRate" type="{http://www.w3.org/2001/XMLSchema}double" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "StreamRateType", propOrder = {
    "streamQName",
    "minimumFlowRate",
    "averageFlowRate",
    "maximumFlowRate"
})
public class StreamRateType {

    @XmlElement(name = "StreamQName", required = true)
    protected QName streamQName;
    @XmlElement(name = "MinimumFlowRate")
    protected Double minimumFlowRate;
    @XmlElement(name = "AverageFlowRate")
    protected Double averageFlowRate;
    @XmlElement(name = "MaximumFlowRate")
    protected Double maximumFlowRate;

    /**
     * Gets the value of the streamQName property.
     * 
     * @return
     *     possible object is
     *     {@link QName }
     *     
     */
    public QName getStreamQName() {
        return streamQName;
    }

    /**
     * Sets the value of the streamQName property.
     * 
     * @param value
     *     allowed object is
     *     {@link QName }
     *     
     */
    public void setStreamQName(QName value) {
        this.streamQName = value;
    }

    /**
     * Gets the value of the minimumFlowRate property.
     * 
     * @return
     *     possible object is
     *     {@link Double }
     *     
     */
    public Double getMinimumFlowRate() {
        return minimumFlowRate;
    }

    /**
     * Sets the value of the minimumFlowRate property.
     * 
     * @param value
     *     allowed object is
     *     {@link Double }
     *     
     */
    public void setMinimumFlowRate(Double value) {
        this.minimumFlowRate = value;
    }

    /**
     * Gets the value of the averageFlowRate property.
     * 
     * @return
     *     possible object is
     *     {@link Double }
     *     
     */
    public Double getAverageFlowRate() {
        return averageFlowRate;
    }

    /**
     * Sets the value of the averageFlowRate property.
     * 
     * @param value
     *     allowed object is
     *     {@link Double }
     *     
     */
    public void setAverageFlowRate(Double value) {
        this.averageFlowRate = value;
    }

    /**
     * Gets the value of the maximumFlowRate property.
     * 
     * @return
     *     possible object is
     *     {@link Double }
     *     
     */
    public Double getMaximumFlowRate() {
        return maximumFlowRate;
    }

    /**
     * Sets the value of the maximumFlowRate property.
     * 
     * @param value
     *     allowed object is
     *     {@link Double }
     *     
     */
    public void setMaximumFlowRate(Double value) {
        this.maximumFlowRate = value;
    }

}
