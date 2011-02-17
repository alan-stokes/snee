
package eu.semsorgrid4env.service.stream.pull;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.datatype.Duration;


/**
 * <p>Java class for StreamHistoryType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="StreamHistoryType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull}MaximumHistoryDuration" minOccurs="0"/>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull}MaximumHistoryTuples" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "StreamHistoryType", propOrder = {
    "maximumHistoryDuration",
    "maximumHistoryTuples"
})
public class StreamHistoryType {

    @XmlElement(name = "MaximumHistoryDuration")
    protected Duration maximumHistoryDuration;
    @XmlElement(name = "MaximumHistoryTuples")
    protected Integer maximumHistoryTuples;

    /**
     * Gets the value of the maximumHistoryDuration property.
     * 
     * @return
     *     possible object is
     *     {@link Duration }
     *     
     */
    public Duration getMaximumHistoryDuration() {
        return maximumHistoryDuration;
    }

    /**
     * Sets the value of the maximumHistoryDuration property.
     * 
     * @param value
     *     allowed object is
     *     {@link Duration }
     *     
     */
    public void setMaximumHistoryDuration(Duration value) {
        this.maximumHistoryDuration = value;
    }

    /**
     * Gets the value of the maximumHistoryTuples property.
     * 
     * @return
     *     possible object is
     *     {@link Integer }
     *     
     */
    public Integer getMaximumHistoryTuples() {
        return maximumHistoryTuples;
    }

    /**
     * Sets the value of the maximumHistoryTuples property.
     * 
     * @param value
     *     allowed object is
     *     {@link Integer }
     *     
     */
    public void setMaximumHistoryTuples(Integer value) {
        this.maximumHistoryTuples = value;
    }

}
