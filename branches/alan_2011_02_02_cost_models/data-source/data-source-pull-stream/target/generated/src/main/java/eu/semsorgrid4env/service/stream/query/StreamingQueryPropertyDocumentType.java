
package eu.semsorgrid4env.service.stream.query;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.datatype.Duration;
import eu.semsorgrid4env.service.stream.StreamPropertyDocumentType;


/**
 * <p>Java class for StreamingQueryPropertyDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="StreamingQueryPropertyDocumentType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS}StreamPropertyDocumentType">
 *       &lt;sequence>
 *         &lt;element name="MaximumDirectQueryDuration" type="{http://www.w3.org/2001/XMLSchema}duration"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "StreamingQueryPropertyDocumentType", propOrder = {
    "maximumDirectQueryDuration"
})
public class StreamingQueryPropertyDocumentType
    extends StreamPropertyDocumentType
{

    @XmlElement(name = "MaximumDirectQueryDuration", required = true)
    protected Duration maximumDirectQueryDuration;

    /**
     * Gets the value of the maximumDirectQueryDuration property.
     * 
     * @return
     *     possible object is
     *     {@link Duration }
     *     
     */
    public Duration getMaximumDirectQueryDuration() {
        return maximumDirectQueryDuration;
    }

    /**
     * Sets the value of the maximumDirectQueryDuration property.
     * 
     * @param value
     *     allowed object is
     *     {@link Duration }
     *     
     */
    public void setMaximumDirectQueryDuration(Duration value) {
        this.maximumDirectQueryDuration = value;
    }

}
