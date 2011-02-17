
package eu.semsorgrid4env.service.wsdai;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.namespace.QName;
import org.apache.cxf.ws.addressing.EndpointReferenceType;


/**
 * <p>Java class for FactoryRequestType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="FactoryRequestType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.ggf.org/namespaces/2005/12/WS-DAI}BaseRequestType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}PortTypeQName" minOccurs="0"/>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}ConfigurationDocument" minOccurs="0"/>
 *         &lt;element name="PreferredTargetService" type="{http://www.w3.org/2005/08/addressing}EndpointReferenceType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FactoryRequestType", propOrder = {
    "portTypeQName",
    "configurationDocument",
    "preferredTargetService"
})
public class FactoryRequestType
    extends BaseRequestType
{

    @XmlElement(name = "PortTypeQName")
    protected QName portTypeQName;
    @XmlElement(name = "ConfigurationDocument")
    protected ConfigurationDocumentType configurationDocument;
    @XmlElement(name = "PreferredTargetService", namespace = "")
    protected EndpointReferenceType preferredTargetService;

    /**
     * Gets the value of the portTypeQName property.
     * 
     * @return
     *     possible object is
     *     {@link QName }
     *     
     */
    public QName getPortTypeQName() {
        return portTypeQName;
    }

    /**
     * Sets the value of the portTypeQName property.
     * 
     * @param value
     *     allowed object is
     *     {@link QName }
     *     
     */
    public void setPortTypeQName(QName value) {
        this.portTypeQName = value;
    }

    /**
     * Gets the value of the configurationDocument property.
     * 
     * @return
     *     possible object is
     *     {@link ConfigurationDocumentType }
     *     
     */
    public ConfigurationDocumentType getConfigurationDocument() {
        return configurationDocument;
    }

    /**
     * Sets the value of the configurationDocument property.
     * 
     * @param value
     *     allowed object is
     *     {@link ConfigurationDocumentType }
     *     
     */
    public void setConfigurationDocument(ConfigurationDocumentType value) {
        this.configurationDocument = value;
    }

    /**
     * Gets the value of the preferredTargetService property.
     * 
     * @return
     *     possible object is
     *     {@link EndpointReferenceType }
     *     
     */
    public EndpointReferenceType getPreferredTargetService() {
        return preferredTargetService;
    }

    /**
     * Sets the value of the preferredTargetService property.
     * 
     * @param value
     *     allowed object is
     *     {@link EndpointReferenceType }
     *     
     */
    public void setPreferredTargetService(EndpointReferenceType value) {
        this.preferredTargetService = value;
    }

}
