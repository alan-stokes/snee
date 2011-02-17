
package eu.semsorgrid4env.service.stream.pull;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import eu.semsorgrid4env.service.stream.StreamPropertyDocumentType;


/**
 * <p>Java class for PullStreamPropertyDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="PullStreamPropertyDocumentType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS}StreamPropertyDocumentType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull}StreamHistory"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PullStreamPropertyDocumentType", propOrder = {
    "streamHistory"
})
public class PullStreamPropertyDocumentType
    extends StreamPropertyDocumentType
{

    @XmlElement(name = "StreamHistory", required = true)
    protected StreamHistoryType streamHistory;

    /**
     * Gets the value of the streamHistory property.
     * 
     * @return
     *     possible object is
     *     {@link StreamHistoryType }
     *     
     */
    public StreamHistoryType getStreamHistory() {
        return streamHistory;
    }

    /**
     * Sets the value of the streamHistory property.
     * 
     * @param value
     *     allowed object is
     *     {@link StreamHistoryType }
     *     
     */
    public void setStreamHistory(StreamHistoryType value) {
        this.streamHistory = value;
    }

}
