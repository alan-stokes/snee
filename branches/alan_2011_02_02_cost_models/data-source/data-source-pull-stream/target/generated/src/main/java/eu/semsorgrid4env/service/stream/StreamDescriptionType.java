
package eu.semsorgrid4env.service.stream;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for StreamDescriptionType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="StreamDescriptionType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="StreamDescriptionFormatURI" type="{http://www.w3.org/2001/XMLSchema}anyURI"/>
 *         &lt;element name="StreamDescriptionDocument" type="{http://www.w3.org/2001/XMLSchema}anyType"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "StreamDescriptionType", propOrder = {
    "streamDescriptionFormatURI",
    "streamDescriptionDocument"
})
public class StreamDescriptionType {

    @XmlElement(name = "StreamDescriptionFormatURI", required = true)
    @XmlSchemaType(name = "anyURI")
    protected String streamDescriptionFormatURI;
    @XmlElement(name = "StreamDescriptionDocument", required = true)
    protected Object streamDescriptionDocument;

    /**
     * Gets the value of the streamDescriptionFormatURI property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getStreamDescriptionFormatURI() {
        return streamDescriptionFormatURI;
    }

    /**
     * Sets the value of the streamDescriptionFormatURI property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setStreamDescriptionFormatURI(String value) {
        this.streamDescriptionFormatURI = value;
    }

    /**
     * Gets the value of the streamDescriptionDocument property.
     * 
     * @return
     *     possible object is
     *     {@link Object }
     *     
     */
    public Object getStreamDescriptionDocument() {
        return streamDescriptionDocument;
    }

    /**
     * Sets the value of the streamDescriptionDocument property.
     * 
     * @param value
     *     allowed object is
     *     {@link Object }
     *     
     */
    public void setStreamDescriptionDocument(Object value) {
        this.streamDescriptionDocument = value;
    }

}
