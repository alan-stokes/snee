
package eu.semsorgrid4env.service.stream;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;
import eu.semsorgrid4env.service.stream.push.PushStreamPropertyDocumentType;
import eu.semsorgrid4env.service.wsdai.PropertyDocumentType;


/**
 * <p>Java class for StreamPropertyDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="StreamPropertyDocumentType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}PropertyDocumentType">
 *       &lt;sequence>
 *         &lt;element name="StreamDescription" type="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS}StreamDescriptionType"/>
 *         &lt;element name="StreamRate" type="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS}StreamRateType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "StreamPropertyDocumentType", propOrder = {
    "streamDescription",
    "streamRate"
})
@XmlSeeAlso({
    PushStreamPropertyDocumentType.class
})
public class StreamPropertyDocumentType
    extends PropertyDocumentType
{

    @XmlElement(name = "StreamDescription", required = true)
    protected StreamDescriptionType streamDescription;
    @XmlElement(name = "StreamRate")
    protected List<StreamRateType> streamRate;

    /**
     * Gets the value of the streamDescription property.
     * 
     * @return
     *     possible object is
     *     {@link StreamDescriptionType }
     *     
     */
    public StreamDescriptionType getStreamDescription() {
        return streamDescription;
    }

    /**
     * Sets the value of the streamDescription property.
     * 
     * @param value
     *     allowed object is
     *     {@link StreamDescriptionType }
     *     
     */
    public void setStreamDescription(StreamDescriptionType value) {
        this.streamDescription = value;
    }

    /**
     * Gets the value of the streamRate property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the streamRate property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getStreamRate().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link StreamRateType }
     * 
     * 
     */
    public List<StreamRateType> getStreamRate() {
        if (streamRate == null) {
            streamRate = new ArrayList<StreamRateType>();
        }
        return this.streamRate;
    }

}
