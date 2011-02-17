
package eu.semsorgrid4env.service.stream.pull;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import eu.semsorgrid4env.service.wsdai.RequestType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}RequestType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull}Position" minOccurs="0"/>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull}Count" minOccurs="0"/>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull}MaximumTuples" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "position",
    "count",
    "maximumTuples"
})
@XmlRootElement(name = "GetStreamItemRequest")
public class GetStreamItemRequest
    extends RequestType
{

    @XmlElement(name = "Position")
    protected String position;
    @XmlElement(name = "Count")
    protected String count;
    @XmlElement(name = "MaximumTuples")
    protected Integer maximumTuples;

    /**
     * Gets the value of the position property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPosition() {
        return position;
    }

    /**
     * Sets the value of the position property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPosition(String value) {
        this.position = value;
    }

    /**
     * Gets the value of the count property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCount() {
        return count;
    }

    /**
     * Sets the value of the count property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCount(String value) {
        this.count = value;
    }

    /**
     * Gets the value of the maximumTuples property.
     * 
     * @return
     *     possible object is
     *     {@link Integer }
     *     
     */
    public Integer getMaximumTuples() {
        return maximumTuples;
    }

    /**
     * Sets the value of the maximumTuples property.
     * 
     * @param value
     *     allowed object is
     *     {@link Integer }
     *     
     */
    public void setMaximumTuples(Integer value) {
        this.maximumTuples = value;
    }

}
